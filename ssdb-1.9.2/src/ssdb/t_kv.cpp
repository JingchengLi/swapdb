/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "redis/rdb_encoder.h"
#include "redis/rdb_decoder.h"

extern "C" {
#include "redis/ziplist.h"
#include "redis/intset.h"
};

static bool getNextString(unsigned char *zl, unsigned char **p, std::string &ret_res);

int SSDBImpl::GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv) {
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        return -1;
    } else {
        int ret = kv.DecodeMetaVal(meta_val);
        if (ret < 0) {
            return ret;
        } else if (kv.del == KEY_DELETE_MASK) {
            return 0;
        }
    }
    return 1;
}

int SSDBImpl::SetGeneric(leveldb::WriteBatch &batch, const std::string &key, const std::string &val, int flags, const int64_t expire){
	if (expire < 0){
		return -1;
	}

    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0) {
        return ret;
    } else if (ret == 0) {
        if (flags & OBJ_SET_XX) {
            return -1;
        }
        if (kv.del == KEY_DELETE_MASK) {
            uint16_t version;
            if (kv.version == UINT16_MAX) {
                version = 0;
            } else {
                version = (uint16_t) (kv.version + 1);
            }
            meta_val = encode_kv_val(val, version);
        } else {
            meta_val = encode_kv_val(val);
        }
    } else {
        if (flags & OBJ_SET_NX) {
            return -1;
        }
        meta_val = encode_kv_val(val, kv.version);
    }

	batch.Put(meta_key, meta_val);
	this->edel_one(batch, key);

    return 1;
}

int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset){
	leveldb::WriteBatch batch;
	std::vector<Bytes> lock_key;
	std::vector<Bytes>::const_iterator iter;
    leveldb::Status s;
    int rval = 0;

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);
		mutex_record_.Lock(key.String());
		lock_key.push_back(key);
		int ret = SetGeneric(batch, key.String(), val.String(), OBJ_SET_NO_FLAGS, 0);
		if (ret < 0){
			rval = -1;
			goto return_err;
		}
	}
	s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		rval = -1;
		goto return_err;
	}
	rval = ((int)kvs.size() - offset)/2;

return_err:
	iter = lock_key.begin();
	for (; iter != lock_key.end(); ++iter){
		const Bytes &key = *iter;
		mutex_record_.Unlock(key.String());
	}
	return rval;
}

int SSDBImpl::multi_del(const std::vector<Bytes> &keys, int offset){ //注：redis中不支持该接口
	leveldb::WriteBatch batch;
	std::vector<Bytes> lock_key;
	std::vector<Bytes>::const_iterator iter;

    int num = 0;
	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
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
				if (meta_val[3] == KEY_ENABLED_MASK){
					meta_val[3] = KEY_DELETE_MASK;
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
			num = -1;
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

int SSDBImpl::set(const Bytes &key, const Bytes &val){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

    int ret = SetGeneric(batch, key.String(), val.String(), OBJ_SET_NO_FLAGS, 0);
	if (ret < 0){
		return ret;
	}
	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if (!s.ok()){
		return -1;
	}
	return 1;
}

int SSDBImpl::setnx(const Bytes &key, const Bytes &val){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	int ret = SetGeneric(batch, key.String(), val.String(), OBJ_SET_NX, 0);
	if (ret < 0){
		return ret;
	}
	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if (!s.ok()){
		return -1;
	}
	return 1;
}

int SSDBImpl::getset(const Bytes &key, std::string *val, const Bytes &newval){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	std::string meta_key = encode_meta_key(key);
	std::string meta_val;
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if (ret < 0){
		return ret;
	} else if(ret == 0){
		if (kv.del == KEY_DELETE_MASK){
			uint16_t version;
			if (kv.version == UINT16_MAX){
				version = 0;
			} else{
				version = (uint16_t)(kv.version+1);
			}
			meta_val = encode_kv_val(newval.String(), version);
		} else{
			meta_val = encode_kv_val(newval.String());
		}
	} else{
		meta_val = encode_kv_val(newval.String(), kv.version);
		*val = kv.value;
        this->edel_one(batch, key); //del expire ET key
	}
	batch.Put(meta_key, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return ret;
}

int SSDBImpl::del_key_internal(leveldb::WriteBatch &batch, const Bytes &key) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        return -1;
    } else {
        if (meta_val.size() >= 4) {
            if (meta_val[3] == KEY_ENABLED_MASK) {
                meta_val[3] = KEY_DELETE_MASK;
                uint16_t version = *(uint16_t *) (meta_val.c_str() + 1);
                version = be16toh(version);
                std::string del_key = encode_delete_key(key, version);
                batch.Put(meta_key, meta_val);
                batch.Put(del_key, "");
                this->edel_one(batch, key); //del expire ET key
            } else if (meta_val[3] == KEY_DELETE_MASK){
                return 0;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 1;
}

int SSDBImpl::del(const Bytes &key){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	int ret = del_key_internal(batch, key);
    if (ret <= 0){
        return ret;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }

    return 1;
}

int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	std::string old;
	uint16_t version = 0;
	std::string meta_key = encode_meta_key(key);
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
		if (kv.del == KEY_DELETE_MASK){
			if (kv.version == UINT16_MAX){
				version = 0;
			} else{
				version = (uint16_t)(kv.version+1);
			}
		}
	}else{
		old = kv.value;
		version = kv.version;
        int64_t oldvalue = str_to_int64(old);
        if (errno != 0) {
            return 0;
        }
        if ((by < 0 && oldvalue < 0 && by < (LLONG_MIN - oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (LLONG_MAX - oldvalue))) {
            return 0;
        }
        *new_val = oldvalue + by;
    }

    std::string buf = encode_meta_key(key.String());
    std::string meta_val = encode_kv_val(str(*new_val), version);
	batch.Put(buf, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
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


int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;
	
	std::string val;
	uint16_t version = 0;
	std::string meta_key = encode_meta_key(key);
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		if (kv.del == KEY_DELETE_MASK){
			if (kv.version == UINT16_MAX){
				version = 0;
			} else{
				version = (uint16_t)(kv.version+1);
			}
		}
	}else{
		version = kv.version;
		val = kv.value;
	}

    int len = bitoffset >> 3;
    int bit = 7 - (bitoffset & 0x7);
    if (len >= val.size()) {
        val.resize(len + 1, 0);
    }
    int orig = val[len] & (1 << bit);
    if (on == 1) {
        val[len] |= (1 << bit);
    } else {
        val[len] &= ~(1 << bit);
    }

    std::string buf = encode_meta_key(key.String());
    std::string meta_val = encode_kv_val(val, version);
	batch.Put(buf, meta_val);

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return orig;
}

int SSDBImpl::getbit(const Bytes &key, int bitoffset) {
    std::string val;
    int ret = this->get(key, &val);
    if (ret <= 0) {
        return ret;
    }

    int len = bitoffset >> 3;
    int bit = 7 - (bitoffset & 0x7);
    if (len >= val.size()) {
        return 0;
    }
    return (val[len] & (1 << bit)) == 0 ? 0 : 1;
}


int SSDBImpl::KDel(const Bytes &key){
	RecordLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

    int num = KDelNoLock(batch, key);
    if (num != 1){
        return num;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::KDelNoLock(leveldb::WriteBatch &batch, const Bytes &key){
	std::string buf = encode_meta_key(key.String());
    std::string en_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), buf, &en_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    } else {
        if (en_val[0] != DataType::KV) {
            return 0;
        }
    }
	batch.Delete(buf);
	return 1;
}

/*
 * General API
 */
int SSDBImpl::type(const Bytes &key, std::string *type) {
    *type = "none";
    int ret = 0;
    std::string val;
    std::string meta_key = encode_meta_key(key.String());
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &val);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    }

    if (val[0] == DataType::KV) {
        *type = "string";
        ret = 1;
    } else if (val[0] == DataType::HSIZE) {
        HashMetaVal hv;
        if (hv.DecodeMetaVal(val) == -1) {
            return -1;
        }
        if (hv.del == KEY_ENABLED_MASK) {
            *type = "hash";
            ret = 1;
        }
    } else if (val[0] == DataType::SSIZE) {
        SetMetaVal sv;
        if (sv.DecodeMetaVal(val) == -1) {
            return -1;
        }
        if (sv.del == KEY_ENABLED_MASK) {
            *type = "set";
            ret = 1;
        }
    } else if (val[0] == DataType::ZSIZE) {
        ZSetMetaVal zs;
        if (zs.DecodeMetaVal(val) == -1) {
            return -1;
        }
        if (zs.del == KEY_ENABLED_MASK) {
            *type = "zset";
            ret = 1;
        }
    } else if (val[0] == DataType::LSIZE) {
        ListMetaVal ls;
        if (ls.DecodeMetaVal(val) == -1) {
            return -1;
        }
        if (ls.del == KEY_ENABLED_MASK) {
            *type = "list";
            ret = 1;
        }
    } else {
        return -1;
    }

    return ret;
}

template<typename T>
int decodeMetaVal(T &mv, const std::string &val) {

    if (mv.DecodeMetaVal(val) == -1) {
        return -1;
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
        RecordLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key.String());
        leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
        if (s.IsNotFound()) {
            return 0;
        }
        if (!s.ok()) {
            log_error("get error: %s", s.ToString().c_str());
            return -1;
        }

        //decodeMetaVal
        if(meta_val.size()<4) {
            //invalid
            return -1;
        }

        char del = meta_val[3];
        if (del != KEY_ENABLED_MASK){
            //deleted
            return -1;
        }

        dtype = meta_val[0]; //get snapshot if need
        switch (dtype) {
            case DataType::HSIZE:
            case DataType::SSIZE:
            case DataType::ZSIZE:
            case DataType::LSIZE:{
                snapshot = ldb->GetSnapshot();
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

            auto it = std::unique_ptr<HIterator>(this->hscan_internal(key, "", "", hv.version, -1, snapshot));

            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveRawString(it->val);
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

            auto it = std::unique_ptr<SIterator>(this->sscan_internal(key, "", "", sv.version, -1, snapshot));

            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
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

            auto it = std::unique_ptr<ZIterator>(this->zscan_internal(key, "", "", "", -1, Iterator::FORWARD, zv.version, snapshot));

            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveDoubleValue(it->score);
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

            int64_t rangelen = (int64_t) lv.length;
            uint64_t begin_seq = getSeqByIndex(0, lv);
            uint64_t cur_seq = begin_seq;

            while (rangelen--) {

                std::string item_key = encode_list_key(key, cur_seq, lv.version);
                std::string item_val;
                ret = GetListItemVal(item_key, &item_val, readOptions);
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
            return -1;
    }

    rdbEncoder.encodeFooter();
    *res = rdbEncoder.toString();

    return ret;
}


int SSDBImpl::restore(const Bytes &key, const Bytes &expire, const Bytes &data, bool replace, std::string *res) {
    *res = "none";

    //TODO LOCK

    int ret = 0;
    std::string meta_val;
    leveldb::Status s;

    {
        Transaction trans(binlogs);
        std::string meta_key = encode_meta_key(key.String());
        s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
        if (!s.ok() && !s.IsNotFound()) {
            return -1;
        }

    }

    if (s.ok()) {

        if(meta_val.size()<4) {
            return -1;
        }
        
        char del = meta_val[3];
        if(del == KEY_ENABLED_MASK){
            if (!replace) {
                return -1;
            }

            leveldb::WriteBatch batch;
            del_key_internal(batch, key);
            s = ldb->Write(leveldb::WriteOptions(), &(batch));
            if(!s.ok()){
                return -1;
            }

        } else if (del == KEY_DELETE_MASK){
            //nothing
        } else {
            return -1;
        }

    }

    RdbDecoder rdbDecoder(data.data(), data.size());

    bool ok = rdbDecoder.verifyDumpPayload();
    if (!ok) {
        return -1;
    }

    uint64_t len = 0;

    int rdbtype = rdbDecoder.rdbLoadObjectType();

    log_info("rdb type : %d", rdbtype);

    switch (rdbtype) {
        case RDB_TYPE_STRING: {

            string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return ret;
            }

            set(key, r);

            break;
        }

        case RDB_TYPE_LIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            std::vector<Bytes> val;
            t_res.reserve(len);
            val.reserve(len);

            while (len--) {
                 std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }
                t_res.push_back(std::move(r));
            }

            for (int i = 0; i < t_res.size(); ++i) {
                val.push_back(Bytes(t_res[i]));
            }

            uint64_t len_t;
            RPush(key, val, 0, &len_t);

            break;
        }

        case RDB_TYPE_SET: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            while (len--) {
                //shit
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }

                sadd(key, r);
            }

            break;
        }

        case RDB_TYPE_ZSET_2:
        case RDB_TYPE_ZSET: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            while (len--) {

                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }

                double score;

                if (rdbtype == RDB_TYPE_ZSET_2) {
                    if (rdbDecoder.rdbLoadBinaryDoubleValue(&score) == -1) return -1;
                } else {
                    if (rdbDecoder.rdbLoadDoubleValue(&score) == -1) return -1;
                }

                zset(key, r, str(score));
            }

            break;
        }

        case RDB_TYPE_HASH: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;
            while (len--) {

                std::string field = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }

                std::string value = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }

                hset(key, field, value);
            }

            break;
        }

        case RDB_TYPE_LIST_QUICKLIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            std::vector<Bytes> val;
            t_res.reserve(len);
            val.reserve(len);

            while (len--) {

                std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return ret;
                }

//                log_debug(" zipListStr %s", hexmem(zipListStr.data(), zipListStr.size()).c_str());

                unsigned char *zl = (unsigned char *) zipListStr.data();
                unsigned char *p = ziplistIndex(zl, 0);

                std::string t_item;
                while (getNextString(zl, &p, t_item)) {
//                    log_debug(" item : %s", hexmem(t_item.data(), t_item.length()).c_str());

                    t_res.push_back(std::move(t_item));
                }
            }

            for (int i = 0; i < t_res.size(); ++i) {
                val.push_back(Bytes(t_res[i]));
            }

            uint64_t len_t;
            RPush(key, val, 0, &len_t);

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
                return ret;
            }

            const intset *set = (const intset *)insetStr.data();
            len = intsetLen(set);

//            log_debug(" inset %d %s", len, hexmem(insetStr.data(), insetStr.size()).c_str());

            for (uint32_t j = 0; j < len; ++j) {
                int64_t t_value;
                if (intsetGet((intset *) set, j , &t_value) == 1) {

//                    log_debug("%d : %d", j , t_value);

                    sadd(key, str(t_value));

                } else {
                    return -1;
                }

            }

            break;

        }
        case RDB_TYPE_LIST_ZIPLIST:
        case RDB_TYPE_ZSET_ZIPLIST:
        case RDB_TYPE_HASH_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return ret;
            }

//            log_debug(" zipListStr %s", hexmem(zipListStr.data(), zipListStr.size()).c_str());

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string t_item;
            while (getNextString(zl, &p, t_item)) {
//                log_debug(" item : %s", hexmem(t_item.data(), t_item.length()).c_str());

                if (rdbtype == RDB_TYPE_LIST_ZIPLIST) {

                    std::vector<Bytes> val;
                    val.push_back(Bytes(t_item));
                    uint64_t len_t;
                    RPush(key, val, 0, &len_t);

                } else if (rdbtype == RDB_TYPE_ZSET_ZIPLIST) {
                    std::string value;
                    if(getNextString(zl, &p, value)) {
//                        log_debug(" score : %s", hexmem(value.data(), value.length()).c_str());
                        zset(key, t_item, value);

                    } else {
                        log_error("value not found ");
                        return -1;
                    }

                } else if (rdbtype == RDB_TYPE_HASH_ZIPLIST) {
                    std::string value;
                    if(getNextString(zl, &p, value)) {
//                        log_debug(" value : %s", hexmem(value.data(), value.length()).c_str());
                        hset(key, t_item, value);

                    } else {
                        log_error("value not found ");
                        return -1;
                    }

                } else {
                    log_error("???");
                    return -1;
                }

            }


            break;
        }
//        case RDB_TYPE_MODULE: break;

        default:
            log_error("Unknown RDB encoding type %d", rdbtype);
            return -1;
    }

    *res = "OK";
    ret = 1;
    return ret;
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
            ret_res = std::string((char *)value,sz );
        }

        *p = ziplistNext(zl, *p);
        return true;
    }

    return false;

}
