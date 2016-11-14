/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
// todo r2m adaptation : remove this
//#include "t_hash.h"
#include "ssdb_impl.h"
#include "codec/encode.h"
#include "codec/decode.h"

static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type);
static int hdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type);
static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	Transaction trans(binlogs);

	int ret = hset_one(this, name, key, val, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type){
	Transaction trans(binlogs);

	int ret = hdel_one(this, name, key, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

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

	ret = hset_one(this, name, key, str(*new_val), log_type);
	if(ret == -1){
		return -1;
	}
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
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

int SSDBImpl::HDelKeyNoLock(const Bytes &name, char log_type){
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetHashMetaVal(meta_key, hv);
    if (ret != 1){
        return ret;
    }

    HIterator *it = this->hscan_internal(name, "", "", hv.version, -1);
    int num = 0;
    while(it->next()){
        ret = hdel_one(this, name, it->key, log_type);
        if (-1 == ret){
            return -1;
        } else if (ret > 0){
            num++;
        }
    }
    if (num > 0){
        if(incr_hsize(this, name, -num) == -1){
            return -1;
        }
    }

    return num;
}

int64_t SSDBImpl::hclear(const Bytes &name){
    Transaction trans(binlogs);

    int num = HDelKeyNoLock(name);

    if (num > 0){
        leveldb::Status s = binlogs->commit();
        if (!s.ok()){
            return -1;
        }
    }

    return num;
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
    std::string meta_key = encode_meta_key(name);
    int ret = GetHashMetaVal(meta_key, hv);
    if (0 == ret && hv.del == KEY_DELETE_MASK){
        return hscan_internal(name, start, end, hv.version+1, limit);
    } else if (ret > 0){
        return hscan_internal(name, start, end, hv.version, limit);
    } else{
        return hscan_internal(name, start, end, 0, limit);
    }
}

HIterator* SSDBImpl::hscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version, uint64_t limit){
    std::string key_start, key_end;

    key_start = encode_hash_key(name, start, version);
    if(!end.empty()){
        key_end = encode_hash_key(name, end, version);
    }

    return new HIterator(this->iterator(key_start, key_end, limit), name, version);
}

HIterator* SSDBImpl::hrscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version, uint64_t limit){
    std::string key_start, key_end;

    key_start = encode_hash_key(name, start, version);
    if(start.empty()){
        key_start.append(1, 255);
    }
    if(!end.empty()){
        key_end = encode_hash_key(name, end, version);
    }

    return new HIterator(this->rev_iterator(key_start, key_end, limit), name, version);
}

HIterator* SSDBImpl::hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
    HashMetaVal hv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetHashMetaVal(meta_key, hv);
    if (0 == ret && hv.del == KEY_DELETE_MASK){
        return hrscan_internal(name, start, end, hv.version+1, limit);
    } else if (ret > 0){
        return hrscan_internal(name, start, end, hv.version, limit);
    } else{
        return hrscan_internal(name, start, end, 0, limit);
    }
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

// todo r2m adaptation
int SSDBImpl::hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
/*	std::string start;
	std::string end;
	
	start = encode_hsize_key(name_s);
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}
	
	Iterator *it = this->iterator(start, end, limit);
	get_hnames(it, list);
	delete it;*/
	return 0;
}

// todo r2m adaptation
int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
/*	std::string start;
	std::string end;
	
	start = encode_hsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}
	
	Iterator *it = this->rev_iterator(start, end, limit);
	get_hnames(it, list);
	delete it;*/
	return 0;
}

int SSDBImpl::GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv){
	std::string meta_val;
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
	if (s.IsNotFound()){
		return 0;
	} else if (!s.ok() && !s.IsNotFound()){
		return -1;
	} else{
		int ret = hv.DecodeMetaVal(meta_val);
		if (ret == -1 || hv.type != DataType::HSIZE){
			return -1;
		} else if (hv.del == KEY_DELETE_MASK){
			return 0;
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
static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	int ret = 0;
	std::string dbval;
	HashMetaVal hv;
	std::string meta_key = encode_meta_key(name.String());
	ret = ssdb->GetHashMetaVal(meta_key, hv);
	if (ret == -1){
		return -1;
	} else if (ret == 0 && hv.del == KEY_DELETE_MASK){
		std::string hkey = encode_hash_key(name, key, (uint16_t)(hv.version+1));
		ssdb->binlogs->Put(hkey, slice(val));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		ret = 1;
	} else if (ret == 0){
		std::string hkey = encode_hash_key(name, key);
		ssdb->binlogs->Put(hkey, slice(val));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		ret = 1;
	} else{
		std::string item_key = encode_hash_key(name, key, hv.version);
		ret = ssdb->GetHashItemValInternal(item_key, &dbval);
		if (ret == -1){
			return -1;
		} else if (ret == 0){
			ssdb->binlogs->Put(item_key, slice(val));
			ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, item_key);
			ret = 1;
		} else{
			if(dbval != val){
				ssdb->binlogs->Put(item_key, slice(val));
				ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, item_key);
			}
			ret = 0;
		}
	}
	return ret;
}

static int hdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type){
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
	ssdb->binlogs->Delete(hkey);
	ssdb->binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);
	
	return 1;
}

static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr){
	HashMetaVal hv;
	std::string size_key = encode_meta_key(name);
	int ret = ssdb->GetHashMetaVal(size_key, hv);
	if (ret == -1){
		return ret;
	} else if (ret == 0 && hv.del == KEY_DELETE_MASK){
		if (incr > 0){
			std::string size_val = encode_hash_meta_val((uint64_t)incr, (uint16_t)(hv.version+1));
			ssdb->binlogs->Put(size_key, size_val);
		} else{
			return -1;
		}
	} else if (ret == 0){
		if (incr > 0){
			std::string size_val = encode_hash_meta_val((uint64_t)incr);
			ssdb->binlogs->Put(size_key, size_val);
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
			ssdb->binlogs->Delete(size_key);
		} else{
			std::string size_val = encode_hash_meta_val(len, hv.version);
			ssdb->binlogs->Put(size_key, size_val);
		}
	}

	return 0;
}
