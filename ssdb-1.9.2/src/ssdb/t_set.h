//
// Created by zts on 17-3-30.
//

#ifndef SSDB_T_SADD_H
#define SSDB_T_SADD_H

#include "ssdb_impl.h"

template <typename T>
int SSDBImpl::saddNoLock(Context &ctx, const Bytes &key, const std::set<T> &mem_set, int64_t *num) {
    leveldb::WriteBatch batch;

    int ret = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    ret = GetSetMetaVal(meta_key, sv);
    if (ret < 0) {
        return ret;
    }

    *num = 0;
    typename std::set<T>::const_iterator it = mem_set.begin();
    for (; it != mem_set.end(); ++it) {
        const T &member = *it;
        int change = 0;

        std::string item_key = encode_set_key(key, member, sv.version);
        if (ret == 0) {
            batch.Put(item_key, slice(""));
            change = 1;
        } else {
            int s = GetSetItemValInternal(item_key);
            if (s < 0) {
                return s;
            } else if (s == 1) {
                change = 0;
            } else {
                batch.Put(item_key, slice(""));
                change = 1;
            }
        }
        *num += change;
    }

    int iret = incr_ssize(ctx, key, batch, sv, meta_key, *num);
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if (!s.ok()) {
        log_error("saddNoLock error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}


#endif //SSDB_T_SADD_H
