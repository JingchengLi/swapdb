//
// Created by a1 on 16-11-21.
//
#include <util/error.h>
#include "ssdb_impl.h"


int SSDBImpl::incr_ssize(leveldb::WriteBatch &batch, const SetMetaVal &sv, const std::string &meta_key, const Bytes &key, int64_t incr){

    if (sv.length == 0){
        if (incr > 0){
            std::string size_val = encode_set_meta_val((uint64_t)incr, sv.version);
            batch.Put(meta_key, size_val);
        } else{
            return INVALID_INCR;
        }
    } else {
        uint64_t len = sv.length;
        if (incr > 0) {
            len += incr;
        } else if (incr < 0) {
            uint64_t u64 = static_cast<uint64_t>(-incr);
            if (len < u64) {
                return INVALID_INCR;
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


SIterator* SSDBImpl::sscan_internal(const Bytes &name, const Bytes &start, uint16_t version,
                                    uint64_t limit, const leveldb::Snapshot *snapshot) {

    std::string key_start, key_end;
    key_start = encode_set_key(name, start, version);

    return new SIterator(this->iterator(key_start, key_end, limit, snapshot), name, version);
}


int SSDBImpl::sscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    SetMetaVal sv;

    std::string meta_key = encode_meta_key(name);
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1){
        return ret;
    }
    std::string start;
    if(cursor == "0") {
        start = encode_set_key(name, "", sv.version);
    } else {
        redisCursorService.FindElementByRedisCursor(cursor.String(), start);
    }

    Iterator* iter = this->iterator(start, "", -1);

    auto mit = std::unique_ptr<SIterator>(new SIterator(iter, name, sv.version));

//    bool end = true;
    bool end = doScanGeneric<std::unique_ptr<SIterator>>(mit, pattern, limit, resp);

    if (!end) {
        //get new;
        uint64_t tCursor = redisCursorService.GetNewRedisCursor(iter->key().String()); //we already got it->next
        resp[1] = str(tCursor);
    }

    return 1;


}

int SSDBImpl::saddNoLock(const Bytes &key, const std::set<Bytes> &mem_set, int64_t *num) {
    leveldb::WriteBatch batch;

    int ret = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    ret = GetSetMetaVal(meta_key, sv);
    if (ret < 0){
        return ret;
    }

    *num = 0;
    std::set<Bytes>::const_iterator it = mem_set.begin();
    for (; it != mem_set.end(); ++it) {
        const Bytes& member= *it;
        int change = 0;

        std::string item_key = encode_set_key(key, member, sv.version);
        if (ret == 0){
            batch.Put(item_key, slice(""));
            change = 1;
        } else{
            int s = GetSetItemValInternal(item_key);
            if (s < 0){
                return s;
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
        if((ret = incr_ssize(batch, sv, meta_key, key, *num) < 0)){
            return ret;
        }
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::sadd(const Bytes &key, const std::set<Bytes> &mem_set, int64_t *num) {
    RecordLock<Mutex> l(&mutex_record_, key.String());
    return saddNoLock(key, mem_set, num);
}

int SSDBImpl::srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num) {
    RecordLock<Mutex> l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    int ret = 0;
    *num = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
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
        } else if (s < 0){
            return s;
        }
    }

    if (*num > 0){
        if((ret = incr_ssize(batch, sv, meta_key, key, (-1)*(*num)) < 0)){
            return ret;
        }
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return ret;
}



int SSDBImpl::scard(const Bytes &key, uint64_t *llen) {
    *llen = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret < 0){
        return ret;
    }

    *llen = sv.length;

    return ret;
}

int SSDBImpl::sismember(const Bytes &key, const Bytes &member) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetSetMetaVal(meta_key, sv);
    if (1 == ret){
        std::string hkey = encode_set_key(key, member, sv.version);
        ret = GetSetItemValInternal(hkey);
    }
    return ret;
}

int SSDBImpl::srandmember(const Bytes &key, std::vector<std::string> &members, int64_t cnt) {
    members.clear();
    leveldb::WriteBatch batch;

    const leveldb::Snapshot* snapshot = nullptr;
    SetMetaVal sv;
    int ret;
    {
        RecordLock<Mutex> l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        ret = GetSetMetaVal(meta_key, sv);
        if (ret != 1){
            return ret;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<SIterator>(sscan_internal(key, "", sv.version, -1, snapshot));

    std::set<uint64_t> random_set;
    std::vector<uint64_t> random_list;

    bool allow_repear = cnt < 0;
    if (allow_repear) {
        cnt = -cnt;
    }

    bool full_iter = ((cnt >= sv.length) && (!allow_repear));

    if (sv.length == 0) {
        return 0;
    } else if (full_iter) {
//        for (uint64_t i = 0; i < sv.length; ++i) {
//            if (allow_repear) {
//                random_list.push_back(i);
//            } else {
//                random_set.insert(i);
//            }
//        }
    } else if (cnt > INT_MAX) {
        //TODO random int
        return -1;
    } else {
        if (allow_repear) {
            for (uint64_t i = 0; random_list.size() < cnt; ++i) {
                random_list.push_back((uint64_t)(rand() % sv.length));
            }
        } else {
            for (uint64_t i = 0; random_set.size() < cnt; ++i) {
                //to do rand only produce int !
                random_set.insert((uint64_t)(rand() % sv.length));
            }
        }

    }

    int64_t found_cnt = 0;

    if (full_iter) {
        for (uint64_t i = 0; (it->next()); ++i) {
            members.push_back(std::move(it->key));
        }

    } else if (allow_repear) {
        sort(random_list.begin(), random_list.end());
        for (uint64_t i = 0; (found_cnt < cnt) && (it->next()); ++i) {

            for (uint64_t a : random_list) {
                if (a == i) {
                    members.push_back(it->key);
                    found_cnt += 1;
                }
            }

        }
    } else {

        for (uint64_t i = 0; (found_cnt < cnt) && (it->next()); ++i) {
            std::set<uint64_t >::iterator rit = random_set.find(i);
            if(rit == random_set.end()){
                //not in range
                continue;
            } else {
                random_set.erase(rit);
            }

            members.push_back(std::move(it->key));
            found_cnt += 1;
        }

    }

    return 1;
}

int SSDBImpl::spop(const Bytes &key, std::vector<std::string> &members, int64_t popcnt) {
    members.clear();
    leveldb::WriteBatch batch;

    const leveldb::Snapshot* snapshot = nullptr;
    SetMetaVal sv;
    int ret;

    RecordLock<Mutex> l(&mutex_record_, key.String());

    std::string meta_key = encode_meta_key(key);
    ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1){
        return ret;
    }

    snapshot = ldb->GetSnapshot();
    SnapshotPtr spl(ldb, snapshot); //auto release
    auto it = std::unique_ptr<SIterator>(sscan_internal(key, "", sv.version, -1, snapshot));

    std::set<uint64_t> random_set;
    /* we random key by random index :)
     * in ssdb we only commit once , so make sure not to make same random num */

    if (popcnt < 0) {
        return INDEX_OUT_OF_RANGE;
    }

    if (sv.length == 0) {
        return 0;
    } else if (popcnt >= sv.length) {
        for (uint64_t i = 0; i < sv.length; ++i) {
            random_set.insert(i);
        }
    } else if (popcnt > INT_MAX) {
        //TODO random int
        return -1;
    } else {
        for (uint64_t i = 0; random_set.size() < popcnt; ++i) {
            //to do rand only produce int !
            random_set.insert((uint64_t)(rand() % sv.length));
        }
    }


    int64_t delete_cnt = 0;
    for (uint64_t i = 0; (delete_cnt < popcnt) && (it->next()); ++i) {

        std::set<uint64_t >::iterator rit = random_set.find(i);
        if(rit == random_set.end()){
            //not in range
            continue;
        } else {
            random_set.erase(rit);
        }

        std::string hkey = encode_set_key(key, it->key, sv.version);
        batch.Delete(hkey);
        members.push_back(std::move(it->key));
        delete_cnt += 1;
    }

    if (delete_cnt > 0){
        if((ret = incr_ssize(batch, sv, meta_key, key, (-1)*(delete_cnt)) < 0)){
            return ret;
        }
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::smembers(const Bytes &key, std::vector<std::string> &members) {
    members.clear();

    const leveldb::Snapshot* snapshot = nullptr;
    SetMetaVal sv;
    int s;

    {
        RecordLock<Mutex> l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        s = GetSetMetaVal(meta_key, sv);
        if (s != 1){
            return s;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<SIterator>(sscan_internal(key, "", sv.version, -1, snapshot));
    while (it->next()){
        members.push_back(std::move(it->key));
    }

    return s;
}

int SSDBImpl::sunion_internal(const std::vector<Bytes> &keys, int offset, std::set<std::string> &members) {
    members.clear();
    int size = (int)keys.size();
    for (int i = offset; i < size; ++i) {

        const leveldb::Snapshot* snapshot = nullptr;
        SetMetaVal sv;

        {
            RecordLock<Mutex> l(&mutex_record_, keys[i].String());
            std::string meta_key = encode_meta_key(keys[i]);
            int s = GetSetMetaVal(meta_key, sv);
            if (s < 0){
                members.clear();
                return s;
            } else if (s == 0){
                continue;
            }

            snapshot = ldb->GetSnapshot();
        }

        SnapshotPtr spl(ldb, snapshot); //auto release

        auto it = std::unique_ptr<SIterator>(sscan_internal(keys[i], "", sv.version, -1, snapshot));
        while (it->next()){
            members.insert(it->key);
        }

    }
    return 1;
}

int SSDBImpl::sunion(const std::vector<Bytes> &keys, std::set<std::string> &members) {
    
    
    return sunion_internal(keys, 1, members);
}

int SSDBImpl::sunionstore(const Bytes &destination, const std::vector<Bytes> &keys, int64_t *num) {
//    RecordLock<Mutex> l(&mutex_record_, key.String());//TODO
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
    if (*num == 0) {
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if(!s.ok()){
            return STORAGE_ERR;
        }
        return 1;
    }

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



int SSDBImpl::GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv){
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        //not found
        sv.length = 0;
        sv.del = KEY_ENABLED_MASK;
        sv.type = DataType::HSIZE;
        sv.version = 0;
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        //error
        return STORAGE_ERR;
    } else{
        int ret = sv.DecodeMetaVal(meta_val);
        if (ret < 0){
            //error
            return ret;
        } else if (sv.del == KEY_DELETE_MASK){
            //deleted , reset hv
            if (sv.version == UINT16_MAX){
                sv.version = 0;
            } else{
                sv.version = (uint16_t)(sv.version+1);
            }
            sv.length = 0;
            sv.del = KEY_ENABLED_MASK;
            return 0;
        } else if (sv.type != DataType::SSIZE){
            //error
            return WRONG_TYPE_ERR;
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
        log_error("%s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;
}