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

int SSDBImpl::multi_sadd(const Bytes &key, const std::vector<Bytes> &members, int64_t *num, char log_type) {
    Transaction trans(binlogs);

    int ret = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return -1;
    }

    *num = 0;
    for (int i = 2; i < members.size(); ++i) {
        const Bytes& member= members[i];
        int change = 0;
        if (ret == 0 && sv.del == KEY_DELETE_MASK){
            std::string hkey = encode_set_key(key, member, (uint16_t)(sv.version+1));
            binlogs->Put(hkey, slice(""));
            change = 1;
        } else if (ret == 0){
            std::string hkey = encode_set_key(key, member);
            binlogs->Put(hkey, slice(""));
            change = 1;
        } else{
            std::string item_key = encode_set_key(key, member, sv.version);
            int s = GetSetItemValInternal(item_key);
            if (s == -1){
                return -1;
            } else if (s == 1){
                change = 0;
            } else{
                binlogs->Put(item_key, slice(""));
                change = 1;
            }
        }
        *num += change;
    }

    if (*num > 0){
        if(incr_ssize(key, *num) == -1){
            return -1;
        }
    }
    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        return -1;
    }

    return ret;
}

int SSDBImpl::multi_srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num, char log_type) {
    Transaction trans(binlogs);

    int ret = 0;
    *num = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1){
        return ret;
    }

    for (int i = 2; i < members.size(); ++i){
        const Bytes& member= members[i];
        std::string hkey = encode_set_key(key, member, sv.version);
        int s = GetSetItemValInternal(hkey);
        if (s == 1){
            *num += 1;
            binlogs->Delete(hkey);
        } else if (s == -1){
            return -1;
        }
    }

    if (*num > 0){
        if(incr_ssize(key, (-1)*(*num)) == -1){
            return -1;
        }
    }
    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        return -1;
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

int SSDBImpl::sunion_internal(const std::vector<Bytes> &keys, int offset, std::set<std::string> &members) {
    members.clear();
    int size = (int)keys.size();
    for (int i = offset; i < size; ++i) {
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

int SSDBImpl::sunion(const std::vector<Bytes> &keys, std::set<std::string> &members) {
    Transaction trans(binlogs);

    return sunion_internal(keys, 1, members);
}

int SSDBImpl::sunionstore(const Bytes &destination, const std::vector<Bytes> &keys, int64_t *num, char log_type) {
    Transaction trans(binlogs);

    std::set<std::string> members;
    int ret = sunion_internal(keys, 2, members);
    if (ret == -1){
        return -1;
    }

    std::string key_type;
    ret = type(destination, &key_type);
    if (ret  == -1){
        return -1;
    } else if (ret == 1){
        DelKeyByType(destination, key_type);
    }

    *num = members.size();
    std::set<std::string>::iterator it = members.begin();
    std::string val;
    std::string meta_key = encode_meta_key(destination);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &val);
    if(s.IsNotFound()){
        for (; it != members.end(); ++it) {
            std::string hkey = encode_set_key(destination, *it);
            binlogs->Put(hkey, slice(""));
        }
        std::string size_val = encode_set_meta_val((uint64_t)(*num));
        binlogs->Put(meta_key, size_val);
    } else if(s.ok()){
        if (val[0] == DataType::SSIZE){
            SetMetaVal sv;
            if (sv.DecodeMetaVal(val) == -1){
                return -1;
            }
            for (; it != members.end(); ++it) {
                std::string hkey = encode_set_key(destination, *it, (uint16_t)(sv.version+1));
                binlogs->Put(hkey, slice(""));
            }
            std::string size_val = encode_set_meta_val((uint64_t)(*num), (uint16_t)(sv.version+1));
            binlogs->Put(meta_key, size_val);
        } else{
            for (; it != members.end(); ++it) {
                std::string hkey = encode_set_key(destination, *it);
                binlogs->Put(hkey, slice(""));
            }
            std::string size_val = encode_set_meta_val((uint64_t)(*num));
            binlogs->Put(meta_key, size_val);
        }
    } else if(!s.ok()){
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    }

    s = binlogs->commit();
    if(!s.ok()){
        return -1;
    }

    return 1;
}

int64_t SSDBImpl::SDelKeyNoLock(const Bytes &name, char log_type) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(name);
    int s = GetSetMetaVal(meta_key, sv);
    if (s != 1){
        return s;
    }

    if (sv.length > MAX_NUM_DELETE){
        std::string del_key = encode_delete_key(name, DataType::SSIZE, sv.version);
        std::string meta_val = encode_set_meta_val(sv.length, sv.version, KEY_DELETE_MASK);
        binlogs->Put(del_key, "");
        binlogs->Put(meta_key, meta_val);
        return (int64_t)sv.length;
    }

    SIterator *it = this->sscan_internal(name, "", "", sv.version, -1);
    int num = 0;
    while (it->next()){
        std::string hkey = encode_set_key(name, it->key, sv.version);
        binlogs->Delete(hkey);
        num++;
    }
    if (num > 0){
        if(incr_ssize(name, -num) == -1){
            return -1;
        }
    }

    return num;
}

int64_t SSDBImpl::sclear(const Bytes &name) {
    Transaction trans(binlogs);

    int64_t num = SDelKeyNoLock(name);

    if (num > 0){
        leveldb::Status s = binlogs->commit();
        if (!s.ok()){
            return -1;
        }
    }

    return num;
}
