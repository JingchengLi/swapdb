//
// Created by zts on 17-3-30.
//

#ifndef SSDB_T_HASH_H
#define SSDB_T_HASH_H

#include "ssdb_impl.h"


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

#endif //SSDB_T_HASH_H



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

