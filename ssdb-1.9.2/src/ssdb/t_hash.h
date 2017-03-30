//
// Created by zts on 17-3-30.
//

#ifndef SSDB_T_HASH_H
#define SSDB_T_HASH_H

#include "ssdb_impl.h"


template <typename T>
int SSDBImpl::hmsetNoLock(const Bytes &name, const std::map<T ,T> &kvs, bool check_exists) {

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

    int iret = incr_hsize(batch, meta_key , hv, name, sum);
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        log_error("[hmsetNoLock] error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}


#endif //SSDB_T_HASH_H
