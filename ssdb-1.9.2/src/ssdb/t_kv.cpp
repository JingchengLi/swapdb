/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
// todo r2m adaptation : remove this
#include "t_kv.h"
#include "codec/encode.h"
#include "codec/decode.h"

int SSDBImpl::SetGeneric(const std::string &key, const std::string &val, int flags, const int64_t expire, char log_type){
	if (expire < 0){
		return -1;
	}

	std::string key_type;
	if (type(key, &key_type) == -1){
		return -1;
	}

	if ((flags & OBJ_SET_NX) && (key_type != "none")){
		return -1;
	} else if ((flags & OBJ_SET_XX) && (key_type == "none")){
		return -1;
	}

	Transaction trans(binlogs);

	if (key_type != "none"){
		DelKeyByType(key, key_type);
	}

	if (expire > 0){
        expiration->set_ttl_internal(key, expire);
	}

	std::string meta_key = encode_meta_key(key);
	std::string meta_val = encode_kv_val(val);
	binlogs->Put(meta_key, meta_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, meta_key);
	leveldb::Status s = binlogs->commit();
	if (!s.ok()){
        //todo 时间戳排序回滚fast_keys first_timeout
		return -1;
	}
    return 0;
}

int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return 0;
			//return -1;
		}
		const Bytes &val = *(it + 1);
// todo r2m adaptation
		std::string buf = encode_kv_key(key);
		binlogs->Put(buf, slice(val));
		binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	}
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

int SSDBImpl::multi_del(const std::vector<Bytes> &keys, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
// todo r2m adaptation
		std::string buf = encode_kv_key(key);
		binlogs->Delete(buf);
		binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	}
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_del error: %s", s.ToString().c_str());
		return -1;
	}
	return keys.size() - offset;
}

int SSDBImpl::set(const Bytes &key, const Bytes &val, char log_type){
    return SetGeneric(key.String(), val.String(), OBJ_SET_NO_FLAGS, 0);
/*	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;*/
}

int SSDBImpl::setnx(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

	std::string tmp;
	int found = this->get(key, &tmp);
	if(found != 0){
		return 0;
	}
// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

	int found = this->get(key, val);
// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(newval));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return found;
}


int SSDBImpl::del(const Bytes &key, char log_type){
	Transaction trans(binlogs);

// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Delete(buf);
	binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

	std::string old;
	int ret = this->get(key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
		*new_val = str_to_int64(old) + by;
		if(errno != 0){
			return 0;
		}
	}

// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, str(*new_val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::get(const Bytes &key, std::string *val){
	std::string buf = encode_meta_key(key.String());
    std::string en_val;

	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), buf, &en_val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
    KvMetaVal kv;
    if (kv.DecodeMetaVal(en_val) == -1){
        return -1;
    } else{
        *val = kv.value;
    }
	return 1;
}

// todo r2m adaptation
KIterator* SSDBImpl::scan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;
	key_start = encode_kv_key(start);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));
}

// todo r2m adaptation
KIterator* SSDBImpl::rscan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_kv_key(start);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));
}

int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}
	Transaction trans(binlogs);
	
	std::string val;
	int ret = this->get(key, &val);
	if(ret == -1){
		return -1;
	}
	
	int len = bitoffset / 8;
	int bit = bitoffset % 8;
	if(len >= val.size()){
		val.resize(len + 1, 0);
	}
	int orig = val[len] & (1 << bit);
	if(on == 1){
		val[len] |= (1 << bit);
	}else{
		val[len] &= ~(1 << bit);
	}

// todo r2m adaptation
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, val);
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
	if(ret == -1){
		return -1;
	}
	
	int len = bitoffset / 8;
	int bit = bitoffset % 8;
	if(len >= val.size()){
		return 0;
	}
	return (val[len] & (1 << bit)) == 0? 0 : 1;
}

/*
 * private API
 */
int SSDBImpl::DelKeyByType(const std::string &key, const std::string &type){
	//todo 内部接口，保证操作的原子性，调用No Commit接口
	int ret = 0;
	if ("string" == type){
		ret = KDelNoLock(key);
	} else if ("hash" == type){
//		s = HDelKeyNoLock(key, &res);
	} else if ("list" == type){
//		s = LDelKeyNoLock(key, &res);
	} else if ("set" == type){
//		s = SDelKeyNoLock(key, &res);
	} else if ("zset" == type){
//		s = ZDelKeyNoLock(key, &res);
	}

	return 0;
}

int SSDBImpl::KDelNoLock(const Bytes &key, char log_type){
	std::string buf = encode_meta_key(key.String());
	binlogs->Delete(buf);
	binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	return 0;
}

/*
 * General API
 */
int SSDBImpl::type(const Bytes &key, std::string *type){
	*type = "none";
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
	} else if (val[0] == DataType::HSIZE){
		HashMetaVal hv;
		if (hv.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (hv.del == KEY_ENABLED_MASK){
			*type = "hash";
		}
	} else if (val[0] == DataType::SSIZE){
		SetMetaVal sv;
		if (sv.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (sv.del == KEY_ENABLED_MASK){
			*type = "set";
		}
	} else if (val[0] == DataType::ZSIZE){
		ZSetMetaVal zs;
		if (zs.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (zs.del == KEY_ENABLED_MASK){
			*type = "zset";
		}
	} else if (val[0] == DataType::LSZIE){
		ListMetaVal ls;
		if (ls.DecodeMetaVal(val) == -1){
			return -1;
		}
		if (ls.del == KEY_ENABLED_MASK){
			*type = "list";
		}
	} else{
		return -1;
	}

	return 0;
}
