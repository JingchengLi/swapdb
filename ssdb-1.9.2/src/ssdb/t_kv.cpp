/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <util/error.h>
#include <cfloat>
#include "ssdb_impl.h"
#include "t_kv.h"


int SSDBImpl::GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv) {
    std::string meta_val;
//    bool found = true;

    leveldb::Status s;


#ifdef USE_LEVELDB
    s = ldb->Get(commonRdOpt, meta_key, &meta_val);
#else
//    if (ldb->KeyMayExist(commonRdOpt, meta_key, &meta_val, &found)) {
//        if (!found) {
//            s = ldb->Get(commonRdOpt, meta_key, &meta_val);
//        }
//    } else {
//        s = s.NotFound();
//    }

    s = ldb->Get(commonRdOpt, meta_key, &meta_val);

#endif

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

int SSDBImpl::SetGeneric(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const Bytes &val, int flags, int64_t expire_ms, int *added){
    if (flags & OBJ_SET_EX || flags & OBJ_SET_PX) {
        if (expire_ms <= 0){
            return INVALID_EX_TIME; //NOT USED
        }
    }

    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0) {
        return ret;
    } else if (ret == 0) {
        if (flags & OBJ_SET_XX) {
            //0-0
            *added = 0;
        } else {
            //1-1
            meta_val = encode_kv_val(val, kv.version);
            batch.Put(meta_key, meta_val);
            ret = 1;
            *added = 1;
        }
    } else {
        // ret = 1
        if (flags & OBJ_SET_NX) {
            //1-0
            *added = 0;
        } else {
            //1-1
            meta_val = encode_kv_val(val, kv.version);
            batch.Put(meta_key, meta_val);
            *added = 1;
        }
    }

    //if ret == 1 , key exists
    //if *added , key new add
    if (flags & OBJ_SET_EX || flags & OBJ_SET_PX) {
        //expire set
        if (ret == 1 && (*added == 1)) {
            expiration->expireAt(ctx, key, expire_ms + time_ms(), batch, false);
        }

    } else {
        if (*added == 1) {
            expiration->cancelExpiration(ctx, key, batch); //del expire ET key
        }
    }



    return ret;
}

int SSDBImpl::multi_set(Context &ctx, const std::vector<Bytes> &kvs, int offset){
	leveldb::WriteBatch batch;
	std::set<Bytes> lock_key;
	std::set<Bytes>::const_iterator iter;

//    RecordLocks<Mutex> l(&mutex_record_);

    int rval = 0;

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);
        if (lock_key.find(key) == lock_key.end()){
//            l.Lock(key.String());
            lock_key.insert(key);
            rval++;
        }
        RecordKeyLock l(&mutex_record_, key.String());

        int added = 0;
		int ret = SetGeneric(ctx, key, batch, val, OBJ_SET_NO_FLAGS, 0, &added);
		if (ret < 0){
            return ret;
		}
	}
    leveldb::Status s = CommitBatch(ctx, &(batch));
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
        return STORAGE_ERR;
	}

	return rval;
}

int SSDBImpl::multi_del(Context &ctx, const std::set<std::string> &distinct_keys, int64_t *num){
	leveldb::WriteBatch batch;

    RecordLocks<Mutex> ls(&mutex_record_, distinct_keys);

    for (const auto &key : distinct_keys) {
        int iret = del_key_internal(ctx, key, batch);
        if (iret < 0) {
            return iret;
        }

        *num = (*num) + iret;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("multi_del error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

	return 1;
}


int SSDBImpl::quickKv(Context &ctx, const Bytes &key, const Bytes &val, const std::string &meta_key,
                      const std::string &meta_val, int64_t expire_ms) {

    leveldb::WriteBatch batch;

    uint16_t version = 0;
    if (meta_val.size() > 0) {
        KvMetaVal kv;
        int ret = kv.DecodeMetaVal(meta_val, true);
        if (ret < 0) {
            return ret;
        } else if (kv.del == KEY_DELETE_MASK) {
            if (kv.version == UINT16_MAX) {
                version = 0;
            } else {
                version = (uint16_t) (kv.version + 1);
            }
        }
    }

    std::string new_meta_val = encode_kv_val(val, version);
    batch.Put(meta_key, new_meta_val);

    if (expire_ms > 0) {
        //expire set
        expiration->expireAt(ctx, key, expire_ms + time_ms(), batch, false);
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if (!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;

}

int SSDBImpl::setNoLock(Context &ctx, const Bytes &key, const Bytes &val, int flags, int64_t expire_ms, int *added) {
    leveldb::WriteBatch batch;

    int ret = SetGeneric(ctx, key, batch, val, flags, expire_ms, added);
    if (ret < 0){
        return ret;
    }

//    if (*added > 0) {
        leveldb::Status s = CommitBatch(ctx, &(batch));
        if (!s.ok()){
            log_error("error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }
//    }

    return ((*added + ret) > 0) ? 1 : 0;
}

int SSDBImpl::set(Context &ctx, const Bytes &key, const Bytes &val, int flags, const int64_t expire_ms, int *added) {
	RecordKeyLock l(&mutex_record_, key.String());

    return setNoLock(ctx, key, val, flags, expire_ms, added);
}

int SSDBImpl::getset(Context &ctx, const Bytes &key, std::pair<std::string, bool> &val, const Bytes &newval){
	RecordKeyLock l(&mutex_record_, key.String());
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
        expiration->cancelExpiration(ctx, key, batch); //del expire ET key
	}
	batch.Put(meta_key, meta_val);

	leveldb::Status s = CommitBatch(ctx, &(batch));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return 1;
}

int SSDBImpl::del_key_internal(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::ReadOptions readOptions = leveldb::ReadOptions();
    readOptions.fill_cache = false;

    leveldb::Status s = ldb->Get(readOptions, meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    } else {
        if (meta_val.size() >= 4) {
            return this->mark_key_deleted(ctx, key, batch, meta_key, meta_val);
        } else {
            return MKEY_DECODEC_ERR;
        }
    }
}

int SSDBImpl::mark_key_deleted(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const std::string &meta_key, std::string &meta_val) {

    if (meta_val[POS_DEL] == KEY_ENABLED_MASK) {
        meta_val[POS_DEL] = KEY_DELETE_MASK;
        uint16_t version = *(uint16_t *) (meta_val.c_str() + 1);
        version = be16toh(version);
        std::string del_key = encode_delete_key(key, version);
        batch.Put(meta_key, meta_val);
        batch.Put(del_key, "");
        expiration->cancelExpiration(ctx, key, batch); //del expire ET key

        return 1;
    } else if (meta_val[POS_DEL] == KEY_DELETE_MASK){
        return 0;
    } else {
        return MKEY_DECODEC_ERR;
    }
}


int SSDBImpl::del(Context &ctx, const Bytes &key){
	RecordKeyLock l(&mutex_record_, key.String());
	leveldb::WriteBatch batch;

	int ret = del_key_internal(ctx, key, batch);
    if (ret <= 0){
        return ret;
    }

	leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("[del] update error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}


int SSDBImpl::incrbyfloat(Context &ctx, const Bytes &key, long double by, long double *new_val) {

    auto func = [&](leveldb::WriteBatch &batch, const KvMetaVal &kv, std::string *new_str, int ret) {

        if (ret == 0) {
            *new_val = by;
        } else {
            std::string old = kv.value;

            long double oldvalue = str_to_long_double(old.c_str(), old.size());
            if (errno == EINVAL) {
                return INVALID_DBL;
            }

            if ((by < 0 && oldvalue < 0 && by < (LDBL_MIN - oldvalue)) ||
                (by > 0 && oldvalue > 0 && by > (LDBL_MAX - oldvalue))) {
                return DBL_OVERFLOW;
            }

            *new_val = oldvalue + by;

            if (std::isnan(*new_val) || std::isinf(*new_val)) {
                return INVALID_INCR_PDC_NAN_OR_INF;
            }

        }

        *new_str = str(*new_val);
        return ret;
    };

    return this->updateKvCommon<decltype(func)>(ctx, key, func);
}


int SSDBImpl::incr(Context &ctx, const Bytes &key, int64_t by, int64_t *new_val){

    auto func = [&] (leveldb::WriteBatch &batch, const KvMetaVal &kv, std::string *new_str, int ret) {

        if (ret == 0) {
            *new_val = by;
        } else {
            std::string old = kv.value;
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

        *new_str = str(*new_val);
        return ret;
    };

    return this->updateKvCommon<decltype(func)>(ctx, key, func);
}

int SSDBImpl::get(Context &ctx, const Bytes &key, std::string *val) {
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



int SSDBImpl::append(Context &ctx, const Bytes &key, const Bytes &value, uint64_t *llen) {

    auto func = [&] (leveldb::WriteBatch &batch, KvMetaVal &kv, std::string *new_str, int ret) {

        if (ret == 0) {
            *new_str = value.String();
        } else {
            *new_str = kv.value.append(value.data(), value.size());
        }

        *llen = new_str->size();
        return ret;
    };

    return this->updateKvCommon<decltype(func)>(ctx, key, func);
}



int SSDBImpl::setbit(Context &ctx, const Bytes &key, int64_t bitoffset, int on, int *res){

    auto func = [&](leveldb::WriteBatch &batch, KvMetaVal &kv, std::string *new_str, int ret) {

        if (ret == 0) {
            //nothing
        } else {
            *new_str = kv.value;
        }

        int64_t len = bitoffset >> 3;
        int64_t bit = 7 - (bitoffset & 0x7);
        if (len >= new_str->size()) {
            new_str->resize(len + 1, 0);
        }
        *res = new_str->at(len) & (1 << bit);
        if (on == 1) {
            new_str->at(len)|= (1 << bit);
        } else {
            new_str->at(len) &= ~(1 << bit);
        }

        return ret;
    };

    return this->updateKvCommon<decltype(func)>(ctx, key, func);
}

int SSDBImpl::getbit(Context &ctx, const Bytes &key, int64_t bitoffset, int *res) {
    std::string val;
    int ret = this->get(ctx, key, &val);
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

int SSDBImpl::setrange(Context &ctx, const Bytes &key, int64_t start, const Bytes &value, uint64_t *new_len) {
    RecordKeyLock l(&mutex_record_, key.String());
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

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::getrange(Context &ctx, const Bytes &key, int64_t start, int64_t end, std::pair<std::string, bool> &res) {
    std::string val;
    int ret = this->get(ctx, key, &val);
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
