/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"

static int hset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, const Bytes &val);
static int hdel_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key);
static int incr_hsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val){
	RecordLock l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

	int ret = hset_one(this, batch, name, key, val);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, batch, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key){
	RecordLock l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

	int ret = hdel_one(this, batch, name, key);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, batch, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val){
	RecordLock l(&mutex_record_, name.String());
	leveldb::WriteBatch batch;

	std::string old;
	int ret = this->hget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
        int64_t oldvalue = str_to_int64(old);
        if(errno != 0){
            return 0;
        }
        if ((by < 0 && oldvalue < 0 && by < (LLONG_MIN-oldvalue)) ||
            (by > 0 && oldvalue > 0 && by > (LLONG_MAX-oldvalue))) {
            return 0;
        }
        *new_val = oldvalue + by;
	}

	ret = hset_one(this, batch, name, key, str(*new_val));
	if(ret == -1){
		return -1;
	}
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, batch, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
		if(!s.ok()){
			return -1;
		}
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

int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	int ret = GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}

	std::string dbkey = encode_hash_key(name, key, hv.version);
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), dbkey, val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("%s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
    HashMetaVal hv;
	uint16_t version;

	std::string meta_key = encode_meta_key(name);
    int ret = GetHashMetaVal(meta_key, hv);
	if (0 == ret && hv.del == KEY_DELETE_MASK){
		version = hv.version+(uint16_t)1;
	} else if (ret > 0){
		version = hv.version;
	} else{
		version = 0;
	}

	return hscan_internal(name, start, end, version, limit);
}

HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit, const leveldb::Snapshot** snapshot){
    HashMetaVal hv;
	uint16_t version;

	{
		RecordLock l(&mutex_record_, name.String());
		std::string meta_key = encode_meta_key(name);
		int ret = GetHashMetaVal(meta_key, hv);
		if (0 == ret && hv.del == KEY_DELETE_MASK){
			version = hv.version+(uint16_t)1;
		} else if (ret > 0){
			version = hv.version;
		} else{
			version = 0;
		}

	     *snapshot = ldb->GetSnapshot();

	}

    return hscan_internal(name, start, end, version, limit, *snapshot);
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


/*// todo r2m adaptation //编码规则决定无法支持该操作，redis也不支持该操作
static void get_hnames(Iterator *it, std::vector<std::string> *list){
	while(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] != DataType::HSIZE){
			break;
		}
		std::string n;
		if(decode_hsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
}*/

int SSDBImpl::GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv){
	std::string meta_val;
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
	if (s.IsNotFound()){
		return 0;
	} else if (!s.ok() && !s.IsNotFound()){
		return -1;
	} else{
		int ret = hv.DecodeMetaVal(meta_val);
		if (ret == -1){
			return -1;
		} else if (hv.del == KEY_DELETE_MASK){
			return 0;
		} else if (hv.type != DataType::HSIZE){
			return -1;
		}
	}
	return 1;
}

int SSDBImpl::GetHashItemValInternal(const std::string &item_key, std::string *val){
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), item_key, val);
	if (s.IsNotFound()){
		return 0;
	} else if (!s.ok() && !s.IsNotFound()){
		return -1;
	}
	return 1;
}

// returns the number of newly added items
static int hset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, const Bytes &val){
	int ret = 0;
	std::string dbval;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name.String());
	ret = ssdb->GetHashMetaVal(meta_key, hv);
	if (ret == -1){
		return -1;
	} else if (ret == 0 && hv.del == KEY_DELETE_MASK){
        uint16_t version;
        if (hv.version == UINT16_MAX){
            version = 0;
        } else{
            version = (uint16_t)(hv.version+1);
        }
		std::string hkey = encode_hash_key(name, key, version);
		batch.Put(hkey, slice(val));
		ret = 1;
	} else if (ret == 0){
		std::string hkey = encode_hash_key(name, key);
		batch.Put(hkey, slice(val));
		ret = 1;
	} else{
		std::string item_key = encode_hash_key(name, key, hv.version);
		ret = ssdb->GetHashItemValInternal(item_key, &dbval);
		if (ret == -1){
			return -1;
		} else if (ret == 0){
			batch.Put(item_key, slice(val));
			ret = 1;
		} else{
			if(dbval != val){
				batch.Put(item_key, slice(val));
			}
			ret = 0;
		}
	}
	return ret;
}

static int hdel_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key){
	int ret = 0;
	std::string dbval;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name);
	ret = ssdb->GetHashMetaVal(meta_key, hv);
	if (ret != 1){
		return ret;
	}
	std::string hkey = encode_hash_key(name, key, hv.version);
	ret = ssdb->GetHashItemValInternal(hkey, &dbval);
	if (ret != 1){
		return ret;
	}
	batch.Delete(hkey);
	
	return 1;
}

static int incr_hsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, int64_t incr){
	HashMetaVal hv;
	std::string size_key = encode_meta_key(name);
	int ret = ssdb->GetHashMetaVal(size_key, hv);
	if (ret == -1){
		return ret;
	} else if (ret == 0 && hv.del == KEY_DELETE_MASK){
		if (incr > 0){
            uint16_t version;
            if (hv.version == UINT16_MAX){
                version = 0;
            } else{
                version = (uint16_t)(hv.version+1);
            }
			std::string size_val = encode_hash_meta_val((uint64_t)incr, version);
			batch.Put(size_key, size_val);
		} else{
			return -1;
		}
	} else if (ret == 0){
		if (incr > 0){
			std::string size_val = encode_hash_meta_val((uint64_t)incr);
			batch.Put(size_key, size_val);
		} else{
			return -1;
		}
	} else{
		uint64_t len = hv.length;
		if (incr > 0) {
			len += incr;
		} else if (incr < 0) {
			uint64_t u64 = static_cast<uint64_t>(-incr);
			if (len < u64) {
				return -1;
			}
			len = len - u64;
		}
		if (len == 0){
			std::string del_key = encode_delete_key(name.String(), hv.version);
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
