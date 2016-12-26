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
        if (ret == -1){
            return -1;
        } else if (sv.del == KEY_DELETE_MASK){
            return 0;
        } else if (sv.type != DataType::SSIZE){
            return -1;
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

int SSDBImpl::sadd_one(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &member) {
    int ret = 0;
    std::string dbval;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return -1;
    } else if (ret == 0 && sv.del == KEY_DELETE_MASK){
        uint16_t version;
        if (sv.version == UINT16_MAX){
            version = 0;
        } else{
            version = (uint16_t)(sv.version+1);
        }
        std::string hkey = encode_set_key(key, member, version);
        batch.Put(hkey, slice(""));
        ret = 1;
    } else if (ret == 0){
        std::string hkey = encode_set_key(key, member);
        batch.Put(hkey, slice(""));
        ret = 1;
    } else{
        std::string item_key = encode_set_key(key, member, sv.version);
        ret = GetSetItemValInternal(item_key);
        if (ret == -1){
            return -1;
        } else if (ret == 1){
            ret = 0;
        } else{
            batch.Put(item_key, slice(""));
            ret = 1;
        }
    }
    return ret;
}

int SSDBImpl::srem_one(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &member) {
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
    batch.Delete(hkey);

    return 1;
}

int SSDBImpl::incr_ssize(leveldb::WriteBatch &batch, const Bytes &key, int64_t incr){
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return ret;
    } else if (ret == 0 && sv.del == KEY_DELETE_MASK){
        if (incr > 0){
            uint16_t version;
            if (sv.version == UINT16_MAX){
                version = 0;
            } else{
                version = (uint16_t)(sv.version+1);
            }
            std::string size_val = encode_set_meta_val((uint64_t)incr, version);
            batch.Put(meta_key, size_val);
        } else{
            return -1;
        }
    } else if (ret == 0){
        if (incr > 0){
            std::string size_val = encode_set_meta_val((uint64_t)incr);
            batch.Put(meta_key, size_val);
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
            std::string del_key = encode_delete_key(key, sv.version);
            std::string meta_val = encode_set_meta_val(sv.length, sv.version, KEY_DELETE_MASK);
            batch.Put(del_key, "");
            batch.Put(meta_key, meta_val);
            this->edel_one(batch, key); //del expire ET key
        } else{
            std::string size_val = encode_set_meta_val(len, sv.version);
            batch.Put(meta_key, size_val);
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
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    int ret = sadd_one(batch, key, member);
    if (ret >= 0){
        if(ret > 0){
            if(incr_ssize(batch, key, ret) == -1){
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

int SSDBImpl::multi_sadd(const Bytes &key, const std::vector<Bytes> &members, int64_t *num, char log_type) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    int ret = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key.String());
    ret = GetSetMetaVal(meta_key, sv);
    if (ret == -1){
        return -1;
    }

    std::set<Bytes> mem_set;
    for (int j = 2; j < members.size(); ++j) {
        mem_set.insert(members[j]);
    }

    *num = 0;
    std::set<Bytes>::iterator it = mem_set.begin();
    for (; it != mem_set.end(); ++it) {
        const Bytes& member= *it;
        int change = 0;
        if (ret == 0 && sv.del == KEY_DELETE_MASK){
            uint16_t version;
            if (sv.version == UINT16_MAX){
                version = 0;
            } else{
                version = (uint16_t)(sv.version+1);
            }
            std::string hkey = encode_set_key(key, member, version);
            batch.Put(hkey, slice(""));
            change = 1;
        } else if (ret == 0){
            std::string hkey = encode_set_key(key, member);
            batch.Put(hkey, slice(""));
            change = 1;
        } else{
            std::string item_key = encode_set_key(key, member, sv.version);
            int s = GetSetItemValInternal(item_key);
            if (s == -1){
                return -1;
            } else if (s == 1){
                change = 0;
            } else{
                batch.Put(item_key, slice(""));
                change = 1;
            }
        }
        *num += change;
    }

    if (*num > 0){
        if(incr_ssize(batch, key, *num) == -1){
            return -1;
        }
    }
    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return -1;
    }

    return ret;
}

int SSDBImpl::multi_srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num, char log_type) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

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
            batch.Delete(hkey);
        } else if (s == -1){
            return -1;
        }
    }

    if (*num > 0){
        if(incr_ssize(batch, key, (-1)*(*num)) == -1){
            return -1;
        }
    }
    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return -1;
    }

    return ret;
}

int SSDBImpl::srem(const Bytes &key, const Bytes &member, char log_type) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    int ret = srem_one(batch, key, member);
    if(ret >= 0){
        if(ret > 0){
            if(incr_ssize(batch, key, -ret) == -1){
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
    Transaction trans(binlogs);//TODO

    return sunion_internal(keys, 1, members);
}

int SSDBImpl::sunionstore(const Bytes &destination, const std::vector<Bytes> &keys, int64_t *num, char log_type) {
//    RecordLock l(&mutex_record_, key.String());//TODO
    leveldb::WriteBatch batch;

    std::set<std::string> members;
    int ret = sunion_internal(keys, 2, members);
    if (ret == -1){
        return -1;
    }

    ret = del_key_internal(batch, destination);
    if (ret < 0){
        return ret;
    }

    *num = members.size();
    std::set<std::string>::iterator it = members.begin();
    std::string val;
    std::string meta_key = encode_meta_key(destination);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &val);
    if(s.IsNotFound()){
        for (; it != members.end(); ++it) {
            std::string hkey = encode_set_key(destination, *it);
            batch.Put(hkey, slice(""));
        }
        std::string size_val = encode_set_meta_val((uint64_t)(*num));
        batch.Put(meta_key, size_val);
    } else if(s.ok()){
        uint16_t version = *(uint16_t *)(val.c_str()+1);
        version = be16toh(version);
        if (version == UINT16_MAX){
            version = 0;
        } else{
            version += 1;
        }
        for (; it != members.end(); ++it) {
            std::string hkey = encode_set_key(destination, *it, version);
            batch.Put(hkey, slice(""));
        }
        std::string size_val = encode_set_meta_val((uint64_t)(*num), version);
        batch.Put(meta_key, size_val);
    } else if(!s.ok()){
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    }

    s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return -1;
    }

    return 1;
}

int64_t SSDBImpl::SDelKeyNoLock(leveldb::WriteBatch &batch, const Bytes &name) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(name);
    int s = GetSetMetaVal(meta_key, sv);
    if (s != 1){
        return s;
    }

    if (sv.length > MAX_NUM_DELETE){
        std::string del_key = encode_delete_key(name, sv.version);
        std::string meta_val = encode_set_meta_val(sv.length, sv.version, KEY_DELETE_MASK);
        batch.Put(del_key, "");
        batch.Put(meta_key, meta_val);
        return (int64_t)sv.length;
    }

    SIterator *it = this->sscan_internal(name, "", "", sv.version, -1);
    int num = 0;
    while (it->next()){
        std::string hkey = encode_set_key(name, it->key, sv.version);
        batch.Delete(hkey);
        num++;
    }
    if (num > 0){
        if(incr_ssize(batch, name, -num) == -1){
            return -1;
        }
    }

    return num;
}

int64_t SSDBImpl::sclear(const Bytes &name) {
    RecordLock l(&mutex_record_, name.String());
    leveldb::WriteBatch batch;

    int64_t num = SDelKeyNoLock(batch, name);

    if (num > 0){
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if (!s.ok()){
            return -1;
        }
    }

    return num;
}
