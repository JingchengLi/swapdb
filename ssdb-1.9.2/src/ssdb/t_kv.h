//
// Created by zts on 17-6-6.
//

#ifndef SSDB_T_KV_H
#define SSDB_T_KV_H


#include "ssdb_impl.h"

template <typename L>
int SSDBImpl::updateKvCommon(Context &ctx, const Bytes &key, L lambda) {

    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    std::string new_val;

    std::string meta_key = encode_meta_key(key);
    KvMetaVal kv;
    int ret = GetKvMetaVal(meta_key, kv);
    if (ret < 0){
        return ret;
    }

    ret = lambda(batch, kv, &new_val, ret);
    if (ret < 0){
        return ret;
    }

    std::string buf = encode_meta_key(key);
    std::string meta_val = encode_kv_val(Bytes(new_val), kv.version);
    batch.Put(buf, meta_val);

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("updateKvCommon error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;

}



#endif //SSDB_T_KV_H
