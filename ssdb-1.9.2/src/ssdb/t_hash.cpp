/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <cfloat>
#include <util/error.h>
#include "ssdb_impl.h"
#include "../../tests/qa/integration/include/ssdb_test.h"

static int hset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const HashMetaVal &hv, bool check_exists,const Bytes &name, const Bytes &key, const Bytes &val);
static int incr_hsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const std::string &size_key, HashMetaVal &hv, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hmset(const Bytes &name, const std::map<Bytes ,Bytes> &kvs) {
	RecordLock l(&mutex_record_, name.String());
	return hmsetNoLock(name, kvs);
}

int SSDBImpl::hmsetNoLock(const Bytes &name, const std::map<Bytes ,Bytes> &kvs) {

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

		int added = hset_one(this ,batch, hv, true, name, key, val);
		if(added < 0){
			return added;
		}

		if(added > 0){
			sum = sum + added;
		}

	}

 	if(sum != 0) {
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, sum)) < 0) {
			return ret;
		}
	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

	return 1;
}

int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val){
	RecordLock l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    ret = GetHashMetaVal(meta_key, hv);
    if(ret < 0) {
        return ret;
    }

    int added = hset_one(this ,batch, hv, true, name, key, val);
	if(added < 0) {
		return added;
	}

	if(added > 0){
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, added)) < 0) {
			return ret;
		}
	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

	return added;
}

int SSDBImpl::hsetnx(const Bytes &name, const Bytes &key, const Bytes &val){
	RecordLock l(&mutex_record_, name.String());
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
			return 0;
		} else if (ret < 0) {
            return ret;
        }
    }

    //ret == 0

	int added = hset_one(this ,batch, hv, false, name, key, val);
	if(added < 0) {
		return added;
	}

	if(added > 0){
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, added)) < 0) {
			return ret;
		}
	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

	return added;
}


int SSDBImpl::hdel(const Bytes &name, const std::set<Bytes> &fields) {
	RecordLock l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);

	int ret = this->GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}

	int deleted = 0;

	for (auto const &key : fields) {

		std::string dbval;
		std::string hkey = encode_hash_key(name, key, hv.version);

		ret = GetHashItemValInternal(hkey, &dbval);
		if (ret < 0){
			return ret;
		} else if (ret == 1){
			batch.Delete(hkey);
			deleted++;
		}
	}

	if (deleted > 0) {
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, -deleted)) < 0) {
			return ret;
		}
	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

	return deleted;
}


int SSDBImpl::hincrbyfloat(const Bytes &name, const Bytes &key, double by, double *new_val){
    RecordLock l(&mutex_record_, name.String());
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
	} else if (ret == 0) {
        *new_val = by;
    } else {

        double oldvalue = str_to_double(old.c_str(), old.size());
		if (errno == EINVAL){
			return INVALID_DBL;
		}

        if ((by < 0 && oldvalue < 0 && by < (DBL_MAX -oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (DBL_MAX -oldvalue))) {
            return DBL_OVERFLOW;
        }

        *new_val = oldvalue + by;
    }


    int added = hset_one(this ,batch, hv, false, name, key, str(*new_val));
    if(added < 0){
        return added;
    }

    if(added > 0){
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, added)) < 0) {
			return ret;
		}
    }

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

    return 1;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val){
	RecordLock l(&mutex_record_, name.String());
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

    if (ret == 0) {
        *new_val = by;
    } else {
        long long oldvalue;
        if (string2ll(old.c_str(), old.size(), &oldvalue) == 0) {
            return INVALID_INT;
        }
        if ((by < 0 && oldvalue < 0 && by < (MIN_INT64-oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (MAX_INT64-oldvalue))) {
            return INT_OVERFLOW;
        }
        *new_val = oldvalue + by;
    }


	int added = hset_one(this ,batch, hv, false, name, key, str(*new_val));
	if(added < 0){
		return added;
	}

	if(added > 0){
		if ((ret = incr_hsize(this, batch, meta_key , hv, name, added)) < 0) {
			return ret;
		}
	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
	if(!s.ok()){
		return STORAGE_ERR;
	}

	return 1;
}

int64_t SSDBImpl::hsize(const Bytes &name){
	HashMetaVal hv;
	std::string size_key = encode_meta_key(name);
	int ret = GetHashMetaVal(size_key, hv);
	if (ret != 1){
		return ret;
	} else{
		return (int64_t)hv.length;
	}
}

int SSDBImpl::hmget(const Bytes &name, const std::vector<std::string> &reqKeys, std::map<std::string, std::string> *resMap) {
	HashMetaVal hv;
	const leveldb::Snapshot* snapshot = nullptr;

	{
		RecordLock l(&mutex_record_, name.String());
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
			(*resMap)[reqKey] = val;

		} else if (ret == 0) {
			continue;
		} else {
			return ret;
		}

	}

    return 1;
}

int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	int ret = GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}

	std::string hkey = encode_hash_key(name, key, hv.version);
	return GetHashItemValInternal(hkey, val);
}


int SSDBImpl::hgetall(const Bytes &name, std::map<std::string, std::string> &val) {

	HashMetaVal hv;
	const leveldb::Snapshot* snapshot = nullptr;

	{
		RecordLock l(&mutex_record_, name.String());
		std::string meta_key = encode_meta_key(name);
		int ret = GetHashMetaVal(meta_key, hv);
		if (ret != 1){
			return ret;
		}
		snapshot = ldb->GetSnapshot();
	}

	SnapshotPtr spl(ldb, snapshot);

	HIterator* hi = hscan_internal(name, "", "", hv.version, -1, snapshot);
	if (hi == nullptr) {
		return -1;
	}
	auto it = std::unique_ptr<HIterator>(hi);

	while(it->next()){
		val[it->key] = it->val;
 	}

	return 1;
}

HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
    HashMetaVal hv;
	uint16_t version;

	std::string meta_key = encode_meta_key(name);
    int ret = GetHashMetaVal(meta_key, hv);
	if (0 == ret){
		version = hv.version;
	} else if (ret > 0){
		version = hv.version;
	} else{
		version = 0;
	}

	return hscan_internal(name, start, end, version, limit);
}


HIterator* SSDBImpl::hscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version, uint64_t limit,
									const leveldb::Snapshot *snapshot){
    std::string key_start, key_end;

    key_start = encode_hash_key(name, start, version);
    if(!end.empty()){
        key_end = encode_hash_key(name, end, version);
    }

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
		return STORAGE_ERR;
	}
	return 1;
}


static int incr_hsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const std::string &size_key, HashMetaVal &hv, const Bytes &name, int64_t incr) {

    if (hv.length == 0){
		if (incr > 0){
			std::string size_val = encode_hash_meta_val((uint64_t)incr, hv.version);
			batch.Put(size_key, size_val);
		} else{
			return INVALID_INCR;
		}
	} else{
		uint64_t len = hv.length;
		if (incr > 0) {
			len += incr;
		} else if (incr < 0) {
			uint64_t u64 = static_cast<uint64_t>(-incr);
			if (len < u64) {
				return INVALID_INCR; //?
			}
			len = len - u64;
		}
		if (len == 0){
			std::string del_key = encode_delete_key(name, hv.version);
			std::string meta_val = encode_hash_meta_val(hv.length, hv.version, KEY_DELETE_MASK);
			batch.Put(del_key, "");
			batch.Put(size_key, meta_val);
			ssdb->edel_one(batch, name); //del expire ET key
		} else{
			std::string size_val = encode_hash_meta_val(len, hv.version);
			batch.Put(size_key, size_val);
		}
	}

	return 0;
}

int hset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const HashMetaVal &hv, bool check_exists, const Bytes &name,
			 const Bytes &key, const Bytes &val) {
	int ret = 0;
	std::string dbval;

	if (check_exists) {
		std::string item_key = encode_hash_key(name, key, hv.version);
		ret = ssdb->GetHashItemValInternal(item_key, &dbval);
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
