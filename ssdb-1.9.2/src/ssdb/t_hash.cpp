/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <cfloat>
#include <util/error.h>
#include "ssdb_impl.h"

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hmset(const Bytes &name, const std::map<Bytes ,Bytes> &kvs) {
	RecordLock<Mutex> l(&mutex_record_, name.String());
	return hmsetNoLock(name, kvs, true);
}

int SSDBImpl::hmsetNoLock(const Bytes &name, const std::map<Bytes ,Bytes> &kvs, bool check_exists) {

	leveldb::WriteBatch batch;

	int ret = 0;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	ret = this->GetHashMetaVal(meta_key, hv);
	if (ret < 0){
		return ret;
	}

	int sum = 0;

	for(auto const &it : kvs)
	{
		const Bytes &key = it.first;
		const Bytes &val = it.second;

		int added = hset_one(batch, hv, check_exists, name, key, val);
		if(added < 0){
			return added;
		}

		if(added > 0){
			sum = sum + added;
		}

	}

 	int iret = incr_hsize(batch, meta_key , hv, name, sum);
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
	} else {
        ret = 1;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("[hmsetNoLock] error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return ret;
}

int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, int *added){
	RecordLock<Mutex> l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    ret = GetHashMetaVal(meta_key, hv);
    if(ret < 0) {
        return ret;
    }

    *added = hset_one(batch, hv, true, name, key, val);
	if(*added < 0) {
		return *added;
	}

	int iret = incr_hsize(batch, meta_key , hv, name, *added);
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
	} else {
        ret = 1;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return ret;
}

int SSDBImpl::hsetnx(const Bytes &name, const Bytes &key, const Bytes &val, int *added){
	RecordLock<Mutex> l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    ret = GetHashMetaVal(meta_key, hv);
    if(ret < 0) {
        return ret;
    } else if (ret == 1) {
        std::string dbval;
        std::string hkey = encode_hash_key(name, key, hv.version);

        ret = GetHashItemValInternal(hkey, &dbval);
        if (ret == 1) {
            *added = 0;
			return ret;
		} else if (ret < 0) {
            return ret;
        }
    }

    //ret == 0
	*added = hset_one(batch, hv, false, name, key, val);
	if(added < 0) {
		return *added;
	}

	int iret =incr_hsize(batch, meta_key , hv, name, *added);
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
	} else {
        ret = 1;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return ret;
}


int SSDBImpl::hdel(const Bytes &name, const std::set<Bytes> &fields, int *deleted) {
	RecordLock<Mutex> l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);

	int ret = this->GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}

	for (auto const &key : fields) {

		std::string dbval;
		std::string hkey = encode_hash_key(name, key, hv.version);

		ret = GetHashItemValInternal(hkey, &dbval);
		if (ret < 0){
			return ret;
		} else if (ret == 1){
			batch.Delete(hkey);
            (*deleted) = (*deleted)+1;
		}
	}

	int iret = incr_hsize(batch, meta_key , hv, name, -(*deleted));
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
	} else {
        ret = 1;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return ret;
}


int SSDBImpl::hincrbyfloat(const Bytes &name, const Bytes &key, long double by, long double *new_val){
    RecordLock<Mutex> l(&mutex_record_, name.String());
    leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    ret = GetHashMetaVal(meta_key, hv);
    if(ret < 0) {
        return ret;
    }

    std::string old;
    if (ret > 0) {
        std::string hkey = encode_hash_key(name, key, hv.version);
        ret = GetHashItemValInternal(hkey, &old);
    }

	int added = 0;

	if(ret < 0) {
		return ret;
	} else if (ret == 0) {
        *new_val = by;
		added = hset_one(batch, hv, false, name, key, str(*new_val));

	} else {

        long double oldvalue = str_to_long_double(old.c_str(), old.size());
		if (errno == EINVAL){
			return INVALID_DBL;
		}

        if ((by < 0 && oldvalue < 0 && by < (DBL_MAX -oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (DBL_MAX -oldvalue))) {
            return DBL_OVERFLOW;
        }

        *new_val = oldvalue + by;

		if (std::isnan(*new_val) || std::isinf(*new_val)) {
			return INVALID_INCR_PDC_NAN_OR_INF;
		}

		hset_one(batch, hv, false, name, key, str(*new_val));
	}


    if(added < 0){
        return added;
    }

    int iret = incr_hsize(batch, meta_key , hv, name, added);
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
	} else {
        ret = 1;
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

    return ret;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val){
	RecordLock<Mutex> l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    ret = GetHashMetaVal(meta_key, hv);
    if(ret < 0) {
        return ret;
    }

    std::string old;
    if (ret > 0) {
        std::string hkey = encode_hash_key(name, key, hv.version);
        ret = GetHashItemValInternal(hkey, &old);
    }

    if(ret < 0) {
        return ret;
    }

	int added = 0;

    if (ret == 0) {
        *new_val = by;
		added = hset_one(batch, hv, false, name, key, str(*new_val));

	} else {
        long long oldvalue;
        if (string2ll(old.c_str(), old.size(), &oldvalue) == 0) {
            return INVALID_INT;
        }
        if ((by < 0 && oldvalue < 0 && by < (LLONG_MIN-oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (LLONG_MAX-oldvalue))) {
            return INT_OVERFLOW;
        }
        *new_val = oldvalue + by;
		hset_one(batch, hv, false, name, key, str(*new_val));
	}

	if(added < 0){
		return added;
	}

	int iret = incr_hsize(batch, meta_key , hv, name, added);
	if (iret < 0) {
		return iret;
	} else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }


    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}

	return ret;
}

int SSDBImpl::hsize(const Bytes &name, uint64_t *size){
	HashMetaVal hv;
	std::string size_key = encode_meta_key(name);
	int ret = GetHashMetaVal(size_key, hv);
	if (ret != 1){
		return ret;
	} else{
		*size = hv.length;
		return 1;
	}
}

int SSDBImpl::hmget(const Bytes &name, const std::vector<std::string> &reqKeys, std::map<std::string, std::string> &resMap) {
	HashMetaVal hv;
	const leveldb::Snapshot* snapshot = nullptr;

	{
		RecordLock<Mutex> l(&mutex_record_, name.String());
		std::string meta_key = encode_meta_key(name);
		int ret = GetHashMetaVal(meta_key, hv);
		if (ret != 1){
			return ret;
		}
		snapshot = ldb->GetSnapshot();
	}

	SnapshotPtr spl(ldb, snapshot);

	for (const std::string &reqKey : reqKeys) {
		std::string val;
		std::string hkey = encode_hash_key(name, reqKey, hv.version);

		int ret = GetHashItemValInternal(hkey, &val);
		if (ret == 1) {
			resMap[reqKey] = val;
		} else if (ret == 0) {
			continue;
		} else {
			return ret;
		}

	}

    return 1;
}

int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::pair<std::string, bool> &val){
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	int ret = GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}

	std::string hkey = encode_hash_key(name, key, hv.version);

    ret = GetHashItemValInternal(hkey, &val.first);
    if (ret == 0) {
        val.second = false;
    } else if (ret > 0) {
        val.second = true;
    }
    return 1;
}


int SSDBImpl::hgetall(const Bytes &name, std::map<std::string, std::string> &val) {

	HashMetaVal hv;
	const leveldb::Snapshot* snapshot = nullptr;

	{
		RecordLock<Mutex> l(&mutex_record_, name.String());
		std::string meta_key = encode_meta_key(name);
		int ret = GetHashMetaVal(meta_key, hv);
		if (ret != 1){
			return ret;
		}
		snapshot = ldb->GetSnapshot();
	}

	SnapshotPtr spl(ldb, snapshot);

	HIterator* hi = hscan_internal(name, "", hv.version, -1, snapshot);
	if (hi == nullptr) {
		return -1;
	}
	auto it = std::unique_ptr<HIterator>(hi);

	while(it->next()){
		val[it->key] = it->val;
 	}

	return 1;
}

//HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
//    HashMetaVal hv;
//	uint16_t version;
//
//	std::string meta_key = encode_meta_key(name);
//    int ret = GetHashMetaVal(meta_key, hv);
//	if (0 == ret){
//		version = hv.version;
//	} else if (ret > 0){
//		version = hv.version;
//	} else{
//		version = 0;
//	}
//
//	return hscan_internal(name, start, end, version, limit);
//}


HIterator* SSDBImpl::hscan_internal(const Bytes &name, const Bytes &start, uint16_t version, uint64_t limit,
									const leveldb::Snapshot *snapshot){

    std::string key_start, key_end;
    key_start = encode_hash_key(name, start, version);

    return new HIterator(this->iterator(key_start, key_end, limit, snapshot), name, version);
}


int SSDBImpl::GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv){
	std::string meta_val;
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
	if (s.IsNotFound()){
        //not found
		hv.length = 0;
		hv.del = KEY_ENABLED_MASK;
		hv.type = DataType::HSIZE;
		hv.version = 0;
		return 0;
	} else if (!s.ok() && !s.IsNotFound()){
        //error
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	} else{
		int ret = hv.DecodeMetaVal(meta_val);
		if (ret < 0){
            //error
            return ret;
		} else if (hv.del == KEY_DELETE_MASK){
            //deleted , reset hv
            if (hv.version == UINT16_MAX){
                hv.version = 0;
            } else{
                hv.version = (uint16_t)(hv.version+1);
            }
            hv.length = 0;
            hv.del = KEY_ENABLED_MASK;
            return 0;
		} else if (hv.type != DataType::HSIZE){
            //error
            return WRONG_TYPE_ERR;
		}
	}
	return 1;
}

int SSDBImpl::GetHashItemValInternal(const std::string &item_key, std::string *val){
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), item_key, val);
	if (s.IsNotFound()){
		return 0;
	} else if (!s.ok() && !s.IsNotFound()){
		log_error("%s", s.ToString().c_str());
		log_error("error: %s", s.ToString().c_str());
		return STORAGE_ERR;
	}
	return 1;
}


int SSDBImpl::incr_hsize(leveldb::WriteBatch &batch, const std::string &size_key, HashMetaVal &hv, const Bytes &name, int64_t incr) {
	int ret = 1;

    if (hv.length == 0){
		if (incr > 0){
			std::string meta_val = encode_hash_meta_val((uint64_t)incr, hv.version);
			batch.Put(size_key, meta_val);
		} else if (incr == 0) {
			return 0;
		} else{
			return INVALID_INCR_LEN;
		}
	} else{
		uint64_t len = hv.length;
		if (incr > 0) {
			//TODO 溢出检查
			len += incr;
		} else if (incr < 0) {
			uint64_t u64 = static_cast<uint64_t>(-incr);
			if (len < u64) {
				return INVALID_INCR_LEN; //?
			}
			len = len - u64;
		} else {
			//incr == 0
			return ret;
		}
		if (len == 0){
			std::string del_key = encode_delete_key(name, hv.version);
			std::string meta_val = encode_hash_meta_val(hv.length, hv.version, KEY_DELETE_MASK);
			batch.Put(del_key, "");
			batch.Put(size_key, meta_val);

			edel_one(batch, name); //del expire ET key

			ret = 0;
		} else{
			std::string meta_val = encode_hash_meta_val(len, hv.version);
			batch.Put(size_key, meta_val);
		}
	}

	return ret;
}

int SSDBImpl::hset_one(leveldb::WriteBatch &batch, const HashMetaVal &hv, bool check_exists, const Bytes &name,
			 const Bytes &key, const Bytes &val) {
	int ret = 0;
	if (check_exists) {
		std::string dbval;
		std::string item_key = encode_hash_key(name, key, hv.version);
		ret = GetHashItemValInternal(item_key, &dbval);
		if (ret < 0){
			return ret;
		} else if (ret == 0){
			batch.Put(item_key, slice(val));
			ret = 1;
		} else{
			if(dbval != val){
				batch.Put(item_key, slice(val));
			}
			ret = 0;
		}
	} else {
		std::string item_key = encode_hash_key(name, key, hv.version);
		batch.Put(item_key, slice(val));
		ret = 1;
	}

	return ret;

}



int SSDBImpl::hscan(const Bytes &name, const Bytes &cursor, const std::string &pattern, uint64_t limit,
					std::vector<std::string> &resp) {

	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	int ret = GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}


	std::string start;
	if(cursor == "0") {
		start = encode_hash_key(name, "", hv.version);
	} else {
		redisCursorService.FindElementByRedisCursor(cursor.String(), start);
	}

	Iterator* iter = this->iterator(start, "", -1);


	auto mit = std::unique_ptr<HIterator>(new HIterator(iter, name, hv.version));

	bool end = doScanGeneric<std::unique_ptr<HIterator>>(mit, pattern, limit, resp);

	if (!end) {
		//get new;
		uint64_t tCursor = redisCursorService.GetNewRedisCursor(iter->key().String()); //we already got it->next
		resp[1] = str(tCursor);
	}

	return 1;
}
