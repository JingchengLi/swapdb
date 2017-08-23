/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_T_HASH_H
#define SSDB_T_HASH_H

#include "ssdb_impl.h"

template<typename T>
int SSDBImpl::quickHash(Context &ctx, const Bytes &name, const std::string &meta_key, const std::string &meta_val,
                       T lambda) {

    leveldb::WriteBatch batch;

    int ret = 0;
    HashMetaVal hv;
    if (meta_val.size() > 0) {
        ret = hv.DecodeMetaVal(meta_val);
        if (ret < 0) {
            //error
            return ret;
        } else if (hv.del == KEY_DELETE_MASK) {
            //deleted , reset hv
            if (hv.version == UINT16_MAX) {
                hv.version = 0;
            } else {
                hv.version = (uint16_t) (hv.version + 1);
            }
            hv.length = 0;
            hv.del = KEY_ENABLED_MASK;
        }
    }
    hv.type = DataType::HSIZE;


    int sum = 0;

    std::string key;
    std::string val;

    while (true) {
        auto n = lambda(key, val);
        if (n < 0) {
            return n;
        } else if (n == 0) {
            break;
        }

        std::string item_key = encode_hash_key(name, key, hv.version);
        batch.Put(item_key, val);
        sum++;
    }

    int iret = incr_hsize(ctx, name, batch, meta_key, hv, sum);
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("[hmsetNoLock] error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;

}


template <typename T>
int SSDBImpl::hmsetNoLock(Context &ctx, const Bytes &name, const std::map<T ,T> &kvs, bool check_exists) {

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
        const T &key = it.first;
        const T &val = it.second;

        int added = hset_one(batch, hv, check_exists, name, key, val);
        if(added < 0){
            return added;
        }

        if(added > 0){
            sum = sum + added;
        }

    }

    int iret = incr_hsize(ctx, name, batch, meta_key, hv, sum);
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("[hmsetNoLock] error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}




template <typename L>
int SSDBImpl::hincrCommon(Context &ctx, const Bytes &name, const Bytes &key, L lambda) {

    RecordKeyLock l(&mutex_record_, name.String());
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

    int added = lambda(batch, hv, old, ret);

    if(added < 0){
        return added;
    }

    if (ret == 1) {
        added = 0; //reset added = 0
    }

    int iret = incr_hsize(ctx, name, batch, meta_key , hv, added);
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;

}

#endif //SSDB_T_HASH_H
