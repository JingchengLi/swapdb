/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"

int SSDBImpl::GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv) {
	std::string meta_val;
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
	if (s.IsNotFound()){
		return 0;
	} else if (!s.ok()){
		return -1;
	} else{
		int ret = kv.DecodeMetaVal(meta_val);
		if (ret < 0){
			return ret;
		} else if(kv.del == KEY_DELETE_MASK){
			return 0;
		}
	}
	return 1;
}

int SSDBImpl::SetGeneric(const std::string &key, const std::string &val, int flags, const int64_t expire, char log_type){
	if (expire < 0){
		return -1;
	}

	std::string meta_key = encode_meta_key(key);
	std::string meta_val;
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if (ret < 0){
		return ret;
	} else if(ret == 0){
		if (flags & OBJ_SET_XX){
			return -1;
		}
		if (kv.del == KEY_DELETE_MASK){
			meta_val = encode_kv_val(val, (uint16_t)(kv.version+1));
		} else{
			meta_val = encode_kv_val(val);
		}
	} else{
		if (flags & OBJ_SET_NX){
			return -1;
		}
		meta_val = encode_kv_val(val, kv.version);
	}

	binlogs->Put(meta_key, meta_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, meta_key);

    return 1;
}

int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);
		int ret = SetGeneric(key.String(), val.String(), OBJ_SET_NO_FLAGS, 0);
		if (ret < 0){
			return ret;
		}
	}
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

int SSDBImpl::multi_del(const std::vector<Bytes> &keys, int offset, char log_type){ //注：redis中不支持该接口
	Transaction trans(binlogs);

    int num = 0;
	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
		std::string meta_key = encode_meta_key(key);
		std::string meta_val;
		leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
		if (s.IsNotFound()){
			continue;
		} else if (!s.ok()){
			return -1;
		} else{
			if (meta_val.size() >= 4 ){
				if (meta_val[3] == KEY_ENABLED_MASK){
					meta_val[3] = KEY_DELETE_MASK;
					uint16_t version = *(uint16_t *)(meta_val.c_str()+1);
					version = be16toh(version);
					std::string del_key = encode_delete_key(key, meta_val[0], version);
					binlogs->Put(meta_key, meta_val);
					binlogs->Put(del_key, "");
					num++;
				} else{
					continue;
				}
			} else{
				return -1;
			}
		}
	}
	if (num > 0){
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("multi_del error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return num;
}

int SSDBImpl::set(const Bytes &key, const Bytes &val, char log_type){
	Transaction trans(binlogs);

    int ret = SetGeneric(key.String(), val.String(), OBJ_SET_NO_FLAGS, 0);
	if (ret < 0){
		return ret;
	}
	leveldb::Status s = binlogs->commit();
	if (!s.ok()){
		return -1;
	}
	return 1;
}

int SSDBImpl::setnx(const Bytes &key, const Bytes &val, char log_type){
	Transaction trans(binlogs);

	int ret = SetGeneric(key.String(), val.String(), OBJ_SET_NX, 0);
	if (ret < 0){
		return ret;
	}
	leveldb::Status s = binlogs->commit();
	if (!s.ok()){
		return -1;
	}
	return 1;
}

int SSDBImpl::getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type){
    Transaction trans(binlogs);

	std::string meta_key = encode_meta_key(key);
	std::string meta_val;
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if (ret < 0){
		return ret;
	} else if(ret == 0){
		if (kv.del == KEY_DELETE_MASK){
			meta_val = encode_kv_val(newval.String(), (uint16_t)(kv.version+1));
		} else{
			meta_val = encode_kv_val(newval.String());
		}
	} else{
		meta_val = encode_kv_val(newval.String(), kv.version);
		*val = kv.value;
	}
	binlogs->Put(meta_key, meta_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, meta_key);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return ret;
}

int SSDBImpl::del_key_internal(const Bytes &key, char log_type) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok()){
        return -1;
    } else{
        if (meta_val.size() >= 4 ){
            if (meta_val[3] == KEY_ENABLED_MASK){
                meta_val[3] = KEY_DELETE_MASK;
                uint16_t version = *(uint16_t *)(meta_val.c_str()+1);
                version = be16toh(version);
                std::string del_key = encode_delete_key(key, meta_val[0], version);
                binlogs->Put(meta_key, meta_val);
                binlogs->Put(del_key, "");
            } else if (meta_val[3] == KEY_DELETE_MASK){
                return 0;
            } else{
                return -1;
            }
        } else{
            return -1;
        }
    }
}

int SSDBImpl::del(const Bytes &key, char log_type){
    Transaction trans(binlogs);

	int ret = del_key_internal(key, log_type);
    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }

	return 1;
}

int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

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
			version = (uint16_t)(kv.version + 1);
		}
	}else{
		old = kv.value;
		version = kv.version;
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

	std::string buf = encode_meta_key(key.String());
    std::string meta_val = encode_kv_val(str(*new_val), version);
	binlogs->Put(buf, meta_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::get(const Bytes &key, std::string *val){
	std::string meta_key = encode_meta_key(key);
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if (ret <= 0){
		return ret;
	} else{
		*val = kv.value;
	}

	return 1;
}

// todo r2m adaptation
KIterator* SSDBImpl::scan(const Bytes &start, const Bytes &end, uint64_t limit){    //不支持kv scan，redis也不支持
/*	std::string key_start, key_end;
	key_start = encode_kv_key(start);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));*/
	return NULL;
}

// todo r2m adaptation
KIterator* SSDBImpl::rscan(const Bytes &start, const Bytes &end, uint64_t limit){   //不支持kv scan，redis也不支持
	/*std::string key_start, key_end;

	key_start = encode_kv_key(start);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));*/
	return NULL;
}

int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on, char log_type){
	Transaction trans(binlogs);
	
	std::string val;
	uint16_t version = 0;
	std::string meta_key = encode_meta_key(key);
	KvMetaVal kv;
	int ret = GetKvMetaVal(meta_key, kv);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		if (kv.del == KEY_DELETE_MASK){
			version = (uint16_t)(kv.version + 1);
		}
	}else{
		version = kv.version;
		val = kv.value;
	}

    int len = bitoffset >> 3;
    int bit = 7 - (bitoffset & 0x7);
	if(len >= val.size()){
		val.resize(len + 1, 0);
	}
	int orig = val[len] & (1 << bit);
	if(on == 1){
		val[len] |= (1 << bit);
	}else{
		val[len] &= ~(1 << bit);
	}

	std::string buf = encode_meta_key(key.String());
    std::string meta_val = encode_kv_val(val, version);
	binlogs->Put(buf, meta_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return orig;
}

int SSDBImpl::getbit(const Bytes &key, int bitoffset){
	std::string val;
	int ret = this->get(key, &val);
	if(ret <= 0){
		return ret;
	}

    int len = bitoffset >> 3;
    int bit = 7 - (bitoffset & 0x7);
	if(len >= val.size()){
		return 0;
	}
	return (val[len] & (1 << bit)) == 0? 0 : 1;
}

/*
 * private API
 */
int SSDBImpl::DelKeyByType(const Bytes &key, const std::string &type){
	//todo 内部接口，保证操作的原子性，调用No Commit接口
	if ("string" == type){
		KDelNoLock(key);
	} else if ("hash" == type){
		HDelKeyNoLock(key);
	} else if ("list" == type){
//		s = LDelKeyNoLock(key, &res);
	} else if ("set" == type){
		SDelKeyNoLock(key);
	} else if ("zset" == type){
		ZDelKeyNoLock(key);
	}

	return 0;
}

int SSDBImpl::KDel(const Bytes &key, char log_type){
    Transaction trans(binlogs);

    int num = KDelNoLock(key);
    if (num != 1){
        return num;
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::KDelNoLock(const Bytes &key, char log_type){
	std::string buf = encode_meta_key(key.String());
    std::string en_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), buf, &en_val);
    if(s.IsNotFound()){
        return 0;
    } else if(!s.ok()){
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    } else{
        if (en_val[0] != DataType::KV){
            return 0;
        }
    }
	binlogs->Delete(buf);
	binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	return 1;
}

/*
 * General API
 */
int SSDBImpl::type(const Bytes &key, std::string *type){
	*type = "none";
    int ret = 0;
	std::string val;
	std::string meta_key = encode_meta_key(key.String());
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}

	if (val[0] == DataType::KV){
		*type = "string";
        ret = 1;
	} else if (val[0] == DataType::HSIZE){
		HashMetaVal hv;
		if (hv.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (hv.del == KEY_ENABLED_MASK){
			*type = "hash";
            ret = 1;
		}
	} else if (val[0] == DataType::SSIZE){
		SetMetaVal sv;
		if (sv.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (sv.del == KEY_ENABLED_MASK){
			*type = "set";
            ret = 1;
		}
	} else if (val[0] == DataType::ZSIZE){
		ZSetMetaVal zs;
		if (zs.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (zs.del == KEY_ENABLED_MASK){
			*type = "zset";
            ret = 1;
		}
	} else if (val[0] == DataType::LSZIE){
		ListMetaVal ls;
		if (ls.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (ls.del == KEY_ENABLED_MASK){
			*type = "list";
            ret = 1;
		}
	} else{
		return -1;
	}

	return ret;
}
