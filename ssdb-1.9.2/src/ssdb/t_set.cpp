//
// Created by a1 on 16-11-21.
//
#include "ssdb_impl.h"

int SSDBImpl::GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv){
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        return -1;
    } else{
        int ret = sv.DecodeMetaVal(meta_val);
        if (ret == -1 || sv.type != DataType::SSIZE){
            return -1;
        } else if (sv.del == KEY_DELETE_MASK){
            return 0;
        }
    }
    return 1;
}

int SSDBImpl::GetSetItemValInternal(const std::string &item_key){
    std::string val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), item_key, &val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        return -1;
    }
    return 1;
}

int SSDBImpl::sadd_one(const Bytes &key, const Bytes &member, char log_type) {
    int ret = 0;
    std::string dbval;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return -1;
    } else if (ret == 0 && sv.del == KEY_DELETE_MASK){
        std::string hkey = encode_set_key(key, member, (uint16_t)(sv.version+1));
        binlogs->Put(hkey, slice(""));
        ret = 1;
    } else if (ret == 0){
        std::string hkey = encode_set_key(key, member);
        binlogs->Put(hkey, slice(""));
        ret = 1;
    } else{
        std::string item_key = encode_set_key(key, member, sv.version);
        ret = GetSetItemValInternal(item_key);
        if (ret == -1){
            return -1;
        } else if (ret == 1){
            ret = 0;
        } else{
            binlogs->Put(item_key, slice(""));
            ret = 1;
        }
    }
    return ret;
}

int SSDBImpl::srem_one(const Bytes &key, const Bytes &member, char log_type) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1){
        return ret;
    }
    std::string hkey = encode_set_key(key, member, sv.version);
    ret = GetSetItemValInternal(hkey);
    if (ret != 1){
        return ret;
    }
    binlogs->Delete(hkey);
//    binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);

    return 1;
}

int SSDBImpl::incr_ssize(const Bytes &key, int64_t incr){
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return ret;
    } else if (ret == 0 && sv.del == KEY_DELETE_MASK){
        if (incr > 0){
            std::string size_val = encode_set_meta_val((uint64_t)incr, (uint16_t)(sv.version+1));
            binlogs->Put(meta_key, size_val);
        } else{
            return -1;
        }
    } else if (ret == 0){
        if (incr > 0){
            std::string size_val = encode_set_meta_val((uint64_t)incr);
            binlogs->Put(meta_key, size_val);
        } else{
            return -1;
        }
    } else{
        uint64_t len = sv.length;
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
            binlogs->Delete(meta_key);
        } else{
            std::string size_val = encode_set_meta_val(len, sv.version);
            binlogs->Put(meta_key, size_val);
        }
    }

    return 0;
}

SIterator* SSDBImpl::sscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version,
                                    uint64_t limit) {
    std::string key_start, key_end;

    key_start = encode_set_key(name, start, version);
    if(!end.empty()){
        key_end = encode_set_key(name, end, version);
    }

    return new SIterator(this->iterator(key_start, key_end, limit), name, version);
}

int SSDBImpl::sadd(const Bytes &key, const Bytes &member, char log_type){
    Transaction trans(binlogs);

    int ret = sadd_one(key, member, log_type);
    if (ret >= 0){
        if(ret > 0){
            if(incr_ssize(key, ret) == -1){
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

int SSDBImpl::srem(const Bytes &key, const Bytes &member, char log_type) {
    Transaction trans(binlogs);

    int ret = srem_one(key, member, log_type);
    if(ret >= 0){
        if(ret > 0){
            if(incr_ssize(key, -ret) == -1){
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

int SSDBImpl::scard(const Bytes &key, uint64_t *llen) {
    *llen = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int s = GetSetMetaVal(meta_key, sv);
    if (1 == s){
        *llen = sv.length;
    }
    return s;
}

int SSDBImpl::sismember(const Bytes &key, const Bytes &member) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int s = GetSetMetaVal(meta_key, sv);
    if (1 == s){
        std::string hkey = encode_set_key(key, member, sv.version);
        s = GetSetItemValInternal(hkey);
    }
    return s;
}

int SSDBImpl::smembers(const Bytes &key, std::vector<std::string> &members) {
    members.clear();
    SetMetaVal sv;

    std::string meta_key = encode_meta_key(key);
    int s = GetSetMetaVal(meta_key, sv);
    if (s != 1){
        return s;
    }

    SIterator* it = sscan_internal(key, "", "", sv.version, -1);
    while (it->next()){
        members.push_back(it->key);
    }

    delete it;
    it = NULL;
    return s;
}

int SSDBImpl::sunion(const std::vector<Bytes> &keys, std::set<std::string> &members) {
    members.clear();
    int size = (int)keys.size();
    for (int i = 1; i < size; ++i) {
        SetMetaVal sv;
        std::string meta_key = encode_meta_key(keys[i]);
        int s = GetSetMetaVal(meta_key, sv);
        if (s == -1){
            members.clear();
            return -1;
        } else if (s == 0){
            continue;
        } else{
            SIterator* it = sscan_internal(keys[i], "", "", sv.version, -1);
            while (it->next()){
                members.insert(it->key);
            }
            delete it;
            it = NULL;
        }
    }
    return 1;
}
