/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <util/error.h>
#include <cfloat>
#include "ssdb_impl.h"
#include "redis/rdb_encoder.h"
#include "redis/rdb_decoder.h"
#include "t_hash.h"

extern "C" {
#include "redis/ziplist.h"
#include "redis/intset.h"
};

static bool getNextString(unsigned char *zl, unsigned char **p, std::string &ret_res);

int SSDBImpl::GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv) {
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        kv.version = 0;
        kv.del = KEY_ENABLED_MASK;
        kv.type = DataType::KV;
        return 0;

    } else if (!s.ok()) {
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    } else {
        int ret = kv.DecodeMetaVal(meta_val);
        if (ret < 0) {
            return ret;
        } else if (kv.del == KEY_DELETE_MASK) {
            if (kv.version == UINT16_MAX) {
                kv.version = 0;
            } else {
                kv.version = (uint16_t) (kv.version + 1);
            }

            kv.del = KEY_ENABLED_MASK;
            kv.type = DataType::KV;
            return 0;
        } else {
            return 1;
        }
    }
}

int SSDBImpl::SetGeneric(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &val, int flags, const int64_t expire, int *added){
	if (expire < 0){
		return INVALID_EX_TIME; //NOT USED
	}

    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0) {
        return ret;
    } else if (ret == 0) {
        if (flags & OBJ_SET_XX) {
            *added = 0;
        } else {
            meta_val = encode_kv_val(val, kv.version);
            batch.Put(meta_key, meta_val);
            this->edel_one(batch, key);
            ret = 1;
            *added = 1;
        }
    } else {
        // ret = 1
        if (flags & OBJ_SET_NX) {
            *added = 0;
        } else {
            meta_val = encode_kv_val(val, kv.version);
            batch.Put(meta_key, meta_val);
            this->edel_one(batch, key);
            *added = 1;
        }
    }

    return ret;
}

int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset){
	leveldb::WriteBatch batch;
	std::set<Bytes> lock_key;
	std::set<Bytes>::const_iterator iter;
    leveldb::Status s;
    int rval = 0;

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);
        if (lock_key.find(key) == lock_key.end()){
            mutex_record_.Lock(key.String());
            lock_key.insert(key);
            rval++;
        }

        int added = 0;
		int ret = SetGeneric(batch, key, val, OBJ_SET_NO_FLAGS, 0, &added);
		if (ret < 0){
			rval = ret;
			goto return_err;
		}
	}
	s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		rval = STORAGE_ERR;
		goto return_err;
	}

return_err:
	iter = lock_key.begin();
	for (; iter != lock_key.end(); ++iter){
		const Bytes &key = *iter;
		mutex_record_.Unlock(key.String());
	}
	return rval;
}

//TODO return value fix
int SSDBImpl::multi_del(const std::vector<Bytes> &keys, int offset){ //注：redis中不支持该接口
	leveldb::WriteBatch batch;
	std::vector<Bytes> lock_key;
	std::vector<Bytes>::const_iterator iter;
    std::set<Bytes>     distinct_keys;

    int num = 0;
	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
    for(; it != keys.end(); ++it){
        distinct_keys.insert(*it);
    }
    std::set<Bytes>::const_iterator itor = distinct_keys.begin();
	for(; itor != distinct_keys.end(); itor++){
		const Bytes &key = *itor;
		mutex_record_.Lock(key.String());
		lock_key.push_back(key);
		std::string meta_key = encode_meta_key(key);
		std::string meta_val;
		leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
		if (s.IsNotFound()){
			continue;
		} else if (!s.ok()){
			num = -1;
			goto return_err;
		} else{
			if (meta_val.size() >= 4 ){
				if (meta_val[POS_DEL] == KEY_ENABLED_MASK){
					meta_val[POS_DEL] = KEY_DELETE_MASK;
					uint16_t version = *(uint16_t *)(meta_val.c_str()+1);
					version = be16toh(version);
					std::string del_key = encode_delete_key(key, version);
					batch.Put(meta_key, meta_val);
					batch.Put(del_key, "");
                    this->edel_one(batch, key); //del expire ET key
                    num++;
				} else{
					continue;
				}
			} else{
				num = -1;
				goto return_err;
			}
		}
	}
	if (num > 0){
		leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
		if(!s.ok()){
			log_error("multi_del error: %s", s.ToString().c_str());
			num = STORAGE_ERR;
		}
	}

return_err:
	iter = lock_key.begin();
	for (; iter != lock_key.end(); ++iter){
		const Bytes &key = *iter;
		mutex_record_.Unlock(key.String());
	}

	return num;
}


int SSDBImpl::setNoLock(const Bytes &key, const Bytes &val, int flags, int *added) {
    leveldb::WriteBatch batch;

    int ret = SetGeneric(batch, key, val, flags, 0, added);
    if (ret < 0){
        return ret;
    }

    if (*added > 0) {
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if (!s.ok()){
            log_error("error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }
    }

    return ret;
}

int SSDBImpl::set(const Bytes &key, const Bytes &val, int flags, int *added) {
	RecordLock<Mutex> l(&mutex_record_, key.String());

    return setNoLock(key, val, flags, added);
}

int SSDBImpl::getset(const Bytes &key, std::pair<std::string, bool> &val, const Bytes &newval){
	RecordLock<Mutex> l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	std::string meta_key = encode_meta_key(key);
	std::string meta_val;
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if (ret < 0){
		return ret;
	} else if(ret == 0){
        meta_val = encode_kv_val(newval, kv.version);
    } else{
		meta_val = encode_kv_val(newval, kv.version);
		val.first = kv.value;
        val.second = true;
        this->edel_one(batch, key); //del expire ET key
	}
	batch.Put(meta_key, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return 1;
}

int SSDBImpl::del_key_internal(leveldb::WriteBatch &batch, const Bytes &key) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    } else {
        if (meta_val.size() >= 4) {
            return this->mark_key_deleted(batch, key, meta_key, meta_val);
        } else {
            return MKEY_DECODEC_ERR;
        }
    }
}

int SSDBImpl::mark_key_deleted(leveldb::WriteBatch &batch, const Bytes &key, const std::string &meta_key, std::string &meta_val) {

    if (meta_val[POS_DEL] == KEY_ENABLED_MASK) {
        meta_val[POS_DEL] = KEY_DELETE_MASK;
        uint16_t version = *(uint16_t *) (meta_val.c_str() + 1);
        version = be16toh(version);
        std::string del_key = encode_delete_key(key, version);
        batch.Put(meta_key, meta_val);
        batch.Put(del_key, "");
        this->edel_one(batch, key); //del expire ET key

        return 1;
    } else if (meta_val[POS_DEL] == KEY_DELETE_MASK){
        return 0;
    } else {
        return MKEY_DECODEC_ERR;
    }
}


int SSDBImpl::del(const Bytes &key){
	RecordLock<Mutex> l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	int ret = del_key_internal(batch, key);
    if (ret <= 0){
        return ret;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("[del] update error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}


int SSDBImpl::incrbyfloat(const Bytes &key, long double by, long double *new_val) {
    RecordLock<Mutex> l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    std::string old;

    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0){
        return ret;
    } else if (ret == 0) {
        *new_val = by;
    } else {
        old = kv.value;

        long double oldvalue = str_to_long_double(old.c_str(), old.size());
        if (errno == EINVAL){
            return INVALID_DBL;
        }

        if ((by < 0 && oldvalue < 0 && by < (LDBL_MIN -oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (LDBL_MAX -oldvalue))) {
            return DBL_OVERFLOW;
        }

        *new_val = oldvalue + by;

        if (std::isnan(*new_val) || std::isinf(*new_val)) {
            return INVALID_INCR_PDC_NAN_OR_INF;
        }

    }

    std::string buf = encode_meta_key(key);
    std::string meta_val = encode_kv_val(Bytes(str(*new_val)), kv.version);
    batch.Put(buf, meta_val);

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("del error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;
}


int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val){
	RecordLock<Mutex> l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

    std::string old;

    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0){
        return ret;
    } else if (ret == 0) {
        *new_val = by;
    } else {
        old = kv.value;
        long long oldvalue;
        if (string2ll(old.c_str(), old.size(), &oldvalue) == 0) {
            return INVALID_INT;
        }
        if ((by < 0 && oldvalue < 0 && by < (LLONG_MIN - oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (LLONG_MAX - oldvalue))) {
            return INT_OVERFLOW;
        }
        *new_val = oldvalue + by;
    }

    std::string buf = encode_meta_key(key);
    std::string meta_val = encode_kv_val(Bytes(str(*new_val)), kv.version);
	batch.Put(buf, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("incr error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}
	return 1;
}

int SSDBImpl::get(const Bytes &key, std::string *val) {
    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret <= 0) {
        return ret;
    } else {
        *val = kv.value;
    }

    return 1;
}



int SSDBImpl::append(const Bytes &key, const Bytes &value, uint64_t *llen) {
    RecordLock<Mutex> l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    std::string val;

    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if(ret < 0){
        return ret;
    }else if(ret == 0){

        val = value.String();
        *llen = val.size();

    }else{

        val = kv.value.append(value.data(), value.size());
        *llen = val.size();
    }

    std::string meta_val = encode_kv_val(Bytes(val), kv.version);
    batch.Put(meta_key, meta_val);

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;
}



int SSDBImpl::setbit(const Bytes &key, int64_t bitoffset, int on, int *res){
	RecordLock<Mutex> l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	std::string val;

    std::string meta_key = encode_meta_key(key);
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if(ret < 0){
		return ret;
	}else if(ret == 0){
		//nothing
	}else{
		val = kv.value;
	}

    int64_t len = bitoffset >> 3;
    int64_t bit = 7 - (bitoffset & 0x7);
    if (len >= val.size()) {
        val.resize(len + 1, 0);
    }
    *res = val[len] & (1 << bit);
    if (on == 1) {
        val[len] |= (1 << bit);
    } else {
        val[len] &= ~(1 << bit);
    }

    std::string meta_val = encode_kv_val(Bytes(val), kv.version);
	batch.Put(meta_key, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}
	return 1;
}

int SSDBImpl::getbit(const Bytes &key, int64_t bitoffset, int *res) {
    std::string val;
    int ret = this->get(key, &val);
    if (ret < 0) {
        return ret;
    } else if (ret == 0) {
        *res = 0;
        return 1;
    }

    int64_t len = bitoffset >> 3;
    int64_t bit = 7 - (bitoffset & 0x7);
    if (len >= val.size()) {
        *res = 0;
        return 1;
    }

    if ((val[len] & (1 << bit)) == 0) {
        *res = 0;
    } else {
        *res = 1;
    }
    return 1;
}

int SSDBImpl::setrange(const Bytes &key, int64_t start, const Bytes &value, uint64_t *new_len) {
    RecordLock<Mutex> l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    std::string val;

    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if(ret < 0){
        return ret;
    }else if(ret == 0){
        //nothing
    }else{
        val = kv.value;
    }

    if (ret == 0 && value.size() == 0) {
        *new_len = 0;
        return 1;
    }


    if ((start + value.size()) > 512*1024*1024) {
        //string exceeds maximum allowed size (512MB)
        return STRING_OVERMAX;
    }


    int64_t padding = start - val.size();

    if (padding < 0) {

        std::string head = val.substr(0 ,start);
        head.append(value.data(), value.size());

        if ((start + value.size()) < val.size()) {
            std::string tail = val.substr(start + value.size(), val.size());
            head.append(tail);
        }

        val = head;

    } else {

        val.reserve(start + value.size());
        string zero_str(padding, '\0');

        val.append(zero_str.data(), zero_str.size());
        val.append(value.data(), value.size());
    }

    //to do unit int size int64_t

    *new_len = val.size();

    std::string meta_val = encode_kv_val(Bytes(val), kv.version);
    batch.Put(meta_key, meta_val);

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::getrange(const Bytes &key, int64_t start, int64_t end, std::pair<std::string, bool> &res) {
    std::string val;
    int ret = this->get(key, &val);
    if (ret < 0) {
        return ret;
    }

    if (start < 0 && end < 0 && start > end) {
        return 1;
    }
    size_t strlen = val.size();
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end || strlen == 0) {
        return 1;
    } else {
        res.first.assign((char*)(val.c_str()) + start, end-start+1);
        return 1;
    }
}


/*
 * General API
 */
int SSDBImpl::redisCursorCleanup() {
    redisCursorService.ClearExpireRedisCursor();
    return 1;
}

int SSDBImpl::type(const Bytes &key, std::string *type) {
    *type = "none";

    std::string val;
    std::string meta_key = encode_meta_key(key);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &val);

    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    //decodeMetaVal
    if(val.size()<4) {
        //invalid
        log_error("invalid MetaVal: %s", s.ToString().c_str());
        return INVALID_METAVAL;
    }

    char del = val[POS_DEL];
    if (del != KEY_ENABLED_MASK){
        //deleted
        return 0;
    }
    char mtype = val[POS_TYPE];

    if (mtype == DataType::KV) {
        *type = "string";
    } else if (mtype == DataType::HSIZE) {
        *type = "hash";
    } else if (mtype == DataType::SSIZE) {
        *type = "set";
    } else if (mtype == DataType::ZSIZE) {
        *type = "zset";
    } else if (mtype == DataType::LSIZE) {
        *type = "list";
    } else {
        return MKEY_DECODEC_ERR;
    }

    return 1;
}

int SSDBImpl::exists(const Bytes &key) {
    std::string meta_val;
    std::string meta_key = encode_meta_key(key);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    if (meta_val[POS_DEL] == KEY_ENABLED_MASK) {
        return 1;
    } else if (meta_val[POS_DEL] == KEY_DELETE_MASK){
        return 0;
    } else {
        return MKEY_DECODEC_ERR;
    }
}

template<typename T>
int decodeMetaVal(T &mv, const std::string &val) {

    int ret = mv.DecodeMetaVal(val);
    if (ret < 0) {
        return ret;
    }

    return 1;
}

int SSDBImpl::dump(const Bytes &key, std::string *res) {
    *res = "none";

    int ret = 0;
    std::string meta_val;
    char dtype;

    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock<Mutex> l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
        if (s.IsNotFound()) {
            return 0;
        }
        if (!s.ok()) {
            log_error("get error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }

        //decodeMetaVal
        if(meta_val.size()<4) {
            //invalid
            log_error("invalid MetaVal: %s", s.ToString().c_str());
            return INVALID_METAVAL;
        }

        char del = meta_val[POS_DEL];
        if (del != KEY_ENABLED_MASK){
            //deleted
            return 0;
        }

        dtype = meta_val[0]; //get snapshot if need
        switch (dtype) {
            case DataType::HSIZE:
            case DataType::SSIZE:
            case DataType::ZSIZE:
            case DataType::LSIZE:{
                snapshot = GetSnapshot();
                break;
            }

            default:break;
        }
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    RdbEncoder rdbEncoder;

    switch (dtype) {
        case DataType::KV: {
            KvMetaVal kv;
            ret = decodeMetaVal(kv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_STRING);
            rdbEncoder.rdbSaveRawString(kv.value);
            break;
        }
        case DataType::HSIZE: {
            HashMetaVal hv;
            ret = decodeMetaVal(hv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_HASH);
            rdbEncoder.rdbSaveLen(hv.length);

            auto it = std::unique_ptr<HIterator>(this->hscan_internal(key, "", hv.version, -1, snapshot));


            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveRawString(it->val);
                cnt ++;
            }

            if (cnt != hv.length) {
                log_error("hash metakey length dismatch !!!!! found %d, length %d, key %s", cnt, hv.length, hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::SSIZE: {
            SetMetaVal sv;
            ret = decodeMetaVal(sv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_SET);
            rdbEncoder.rdbSaveLen(sv.length);

            auto it = std::unique_ptr<SIterator>(this->sscan_internal(key, "", sv.version, -1, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                cnt ++;
            }

            if (cnt != sv.length) {
                log_error("set metakey length dismatch !!!!! found %d, length %d, key %s", cnt, sv.length, hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::ZSIZE: {
            ZSetMetaVal zv;
            ret = decodeMetaVal(zv, meta_val);
            if (ret != 1) {
                return ret;

            }

            rdbEncoder.rdbSaveType(RDB_TYPE_ZSET);
            rdbEncoder.rdbSaveLen(zv.length);

            auto it = std::unique_ptr<ZIterator>(this->zscan_internal(key, "", "", -1, Iterator::FORWARD, zv.version, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveDoubleValue(it->score);
                cnt ++;
            }

            if (cnt != zv.length) {
                log_error("zset metakey length dismatch !!!!! found %d, length %d, key %s", cnt, zv.length, hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::LSIZE: {
            ListMetaVal lv;
            ret = decodeMetaVal(lv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_LIST);
            rdbEncoder.rdbSaveLen(lv.length);

            //readOptions using snapshot
            leveldb::ReadOptions readOptions = leveldb::ReadOptions();
            readOptions.fill_cache = false;
            readOptions.snapshot = snapshot;

            uint64_t rangelen = lv.length;
            uint64_t begin_seq = getSeqByIndex(0, lv);
            uint64_t cur_seq = begin_seq;

            while (rangelen--) {

                std::string item_key = encode_list_key(key, cur_seq, lv.version);
                std::string item_val;
                ret = GetListItemValInternal(item_key, &item_val, readOptions);
                if (1 != ret) {
                    return -1;
                }
                rdbEncoder.rdbSaveRawString(item_val);

                if (UINT64_MAX == cur_seq) {
                    cur_seq = 0;
                } else {
                    cur_seq++;
                }
            }

            break;
        }
        default:
            return INVALID_METAVAL;
    }

    rdbEncoder.encodeFooter();
    *res = rdbEncoder.toString();

    return 1;
}


int SSDBImpl::restore(const Bytes &key, int64_t expire, const Bytes &data, bool replace, std::string *res) {
    *res = "none";

    RecordLock<Mutex> l(&mutex_record_, key.String());

    int ret = 0;
    std::string meta_val;
    leveldb::Status s;

    std::string meta_key = encode_meta_key(key);
    s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (!s.ok() && !s.IsNotFound()) {
        return STORAGE_ERR;
    }

    if (s.ok()) {

        if(meta_val.size()<4) {
            return INVALID_METAVAL;
        }

        if(meta_val[POS_DEL] == KEY_ENABLED_MASK){
            if (!replace) {
                //exist key no
                return BUSY_KEY_EXISTS;
            }

            leveldb::WriteBatch batch;
            mark_key_deleted(batch, key, meta_key, meta_val);
            s = ldb->Write(leveldb::WriteOptions(), &(batch));

            if(!s.ok()){
                return STORAGE_ERR;
            }

        }

    }

    PTST(Decode , 0.1)
    RdbDecoder rdbDecoder(data.data(), data.size());
    bool ok = rdbDecoder.verifyDumpPayload();
    PTE(Decode, "verifyDumpPayload")

    if (!ok) {
        log_warn("checksum failed %s:%s", hexmem(key.data(), key.size()).c_str(),hexmem(data.data(), data.size()).c_str());
        return INVALID_DUMP_STR;
    }

    uint64_t len = 0;

    int rdbtype = rdbDecoder.rdbLoadObjectType();

//    log_info("rdb type : %d", rdbtype);

    switch (rdbtype) {
        case RDB_TYPE_STRING: {

            string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            int added = 0;
            ret = this->setNoLock(key, r, OBJ_SET_NO_FLAGS, &added);

            break;
        }

        case RDB_TYPE_LIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            t_res.reserve(len);

            while (len--) {
                 std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }
                t_res.push_back(std::move(r));
            }

            uint64_t len_t;
            ret = this->rpushNoLock(key, t_res, 0, &len_t);

            break;
        }

        case RDB_TYPE_SET: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::set<std::string> tmp_set;
            std::set<Bytes> mem_set;

            while (len--) {
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                tmp_set.insert(std::move(r));
            }

            for (auto const &item : tmp_set) {
                mem_set.insert(Bytes(item));
            }

            int64_t num = 0;
            ret = this->saddNoLock(key, mem_set, &num);

            break;
        }

        case RDB_TYPE_ZSET_2:
        case RDB_TYPE_ZSET: {
            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::map<std::string,std::string> tmp_map;
            std::map<Bytes,Bytes> sorted_set;

            while (len--) {
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                double score;
                if (rdbtype == RDB_TYPE_ZSET_2) {
                    if (rdbDecoder.rdbLoadBinaryDoubleValue(&score) == -1) return -1;
                } else {
                    if (rdbDecoder.rdbLoadDoubleValue(&score) == -1) return -1;
                }

                tmp_map[r] = str(score);
            }

            for(auto const &it : tmp_map)
            {
                const std::string &field_key = it.first;
                const std::string &field_value = it.second;

                sorted_set[Bytes(field_key)] = Bytes(field_value);
            }

            ret = zsetNoLock(key, sorted_set, ZADD_NONE);

            break;
        }

        case RDB_TYPE_HASH: {

            PTST(HASH , 0.1)
            std::map<std::string,std::string> tmp_map;

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;
            while (len--) {

                std::string field = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                std::string value = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                tmp_map[field] = value;
            }

            PTE(HASH, "load")

            ret = this->hmsetNoLock<std::string>(key, tmp_map, false);
            PTE(HASH, "hmsetNoLock")

            break;
        }

        case RDB_TYPE_LIST_QUICKLIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            t_res.reserve(len);

            while (len--) {

                std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                unsigned char *zl = (unsigned char *) zipListStr.data();
                unsigned char *p = ziplistIndex(zl, 0);

                std::string t_item;
                while (getNextString(zl, &p, t_item)) {
                    t_res.push_back(std::move(t_item));
                }
            }

            uint64_t len_t;
            ret = this->rpushNoLock(key, t_res, 0, &len_t);

            break;
        }

        case RDB_TYPE_HASH_ZIPMAP: {
            /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
            return -1;
            break;
        }
        case RDB_TYPE_SET_INTSET:{

            std::string insetStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            const intset *set = (const intset *)insetStr.data();
            len = intsetLen(set);

            std::set<std::string> tmp_set;
            std::set<Bytes> mem_set;

            for (uint32_t j = 0; j < len; ++j) {
                int64_t t_value;
                if (intsetGet((intset *) set, j , &t_value) == 1) {

                    tmp_set.insert(str(t_value));

                } else {
                    return -1;
                }
            }

            for (auto const &item : tmp_set) {
                mem_set.insert(Bytes(item));
            }

            int64_t num = 0;
            ret = this->saddNoLock(key, mem_set, &num);

            break;

        }
        case RDB_TYPE_LIST_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            std::vector<std::string> t_res;

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string r;
            while (getNextString(zl, &p, r)) {
                t_res.push_back(std::move(r));
            }

            uint64_t len_t;
            ret = this->rpushNoLock(key, t_res, 0, &len_t);

            break;
        }
        case RDB_TYPE_ZSET_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            std::map<std::string,std::string> tmp_map;
            std::map<Bytes,Bytes> sorted_set;

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string t_item;
            while (getNextString(zl, &p, t_item)) {
                std::string value;
                    if(getNextString(zl, &p, value)) {
                        tmp_map[t_item] = value;
                    } else {
                        log_error("value not found ");
                        return -1;
                    }
            }

            for(auto const &it : tmp_map)
            {
                const std::string &field_key = it.first;
                const std::string &field_value = it.second;

                sorted_set[Bytes(field_key)] = Bytes(field_value);
            }

            ret = zsetNoLock(key, sorted_set, ZADD_NONE);

            break;
        }

        case RDB_TYPE_HASH_ZIPLIST: {

            std::map<std::string,std::string> tmp_map;

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string field;
            while (getNextString(zl, &p, field)) {
                std::string value;

                if(getNextString(zl, &p, value)) {
                    tmp_map[field] = value;
                } else {
                    log_error("value not found ");
                    return -1;
                }
            }

            ret = this->hmsetNoLock(key, tmp_map, false);

            break;
        }
//        case RDB_TYPE_MODULE: break;

        default:
            log_error("Unknown RDB encoding type %d %s:%s", rdbtype, hexmem(key.data(), key.size()).c_str(),(data.data(), data.size()));
            return -1;
    }

    *res = "OK";
    return 1;
}



bool getNextString(unsigned char *zl, unsigned char **p, std::string &ret_res) {

    unsigned char *value;
    unsigned int sz;
    long long longval;

    if (ziplistGet(*p, &value, &sz, &longval)) {
        if (!value) {
            /* Write the longval as a string so we can re-add it */
            //TODO check str with ll2string in redis
            ret_res = str((int64_t) longval);
        } else {
            ret_res = std::string((char *)value, sz);
        }

        *p = ziplistNext(zl, *p);
        return true;
    }

    return false;

}

int SSDBImpl::parse_replic(const std::vector<std::string> &kvs) {
    if (kvs.size()%2 != 0){
        return -1;
    }

    leveldb::WriteBatch batch;
    auto it = kvs.begin();
    for(; it != kvs.end(); it += 2){
        const std::string& key = *it;
        const std::string& val = *(it+1);
        batch.Put(key, val);
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("write leveldb error: %s", s.ToString().c_str());
        return -1;
    }

    return 0;
}


int
SSDBImpl::scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    // ignore cursor

    std::string start;
    if(cursor == "0") {
        start.append(1, DataType::META);
    } else {
        redisCursorService.FindElementByRedisCursor(cursor.String(), start);
    }


    Iterator* iter = iterator(start, "", -1);

    auto mit = std::unique_ptr<MIterator>(new MIterator(iter));

    bool end = doScanGeneric<std::unique_ptr<MIterator>>(mit, pattern, limit, resp);

    if (!end) {
        //get new;
        uint64_t tCursor = redisCursorService.GetNewRedisCursor(iter->key().String()); //we already got it->next
        resp[1] = str(tCursor);
    }

    return 1;

}


template<typename T>
bool doScanGeneric(const T &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}

template<>
bool doScanGeneric(const std::unique_ptr<HIterator> &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
            resp.push_back(mit->val);
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}


template<>
bool doScanGeneric(const std::unique_ptr<ZIterator> &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
            resp.push_back(str(mit->score));
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}

template<>
bool doScanGeneric(const std::unique_ptr<SIterator> &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    bool end = false; //scan end

    bool fulliter = (pattern == "*");
    while(mit->next()){
        if (limit == 0) {
            break; //check limit
        }

        if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
            resp.push_back(mit->key);
        } else {
            //skip
        }
        limit --;
        if (limit == 0) {
            break; //stop now
        }
    }

    if (!mit->next()) { // check iter , and update next as last key
        //scan end
        end = true;
    } else if (limit != 0) {
        //scan end
        end = true;
    }

    return end;
}