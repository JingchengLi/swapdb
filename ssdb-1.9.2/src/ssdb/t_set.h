/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_T_SADD_H
#define SSDB_T_SADD_H

#include "ssdb_impl.h"



template<typename T>
int SSDBImpl::quickSet(Context &ctx, const Bytes &key, const std::string &meta_key, const std::string &meta_val,
                       T lambda) {

    leveldb::WriteBatch batch;

    int ret = 0;
    SetMetaVal sv;
    if (meta_val.size() > 0) {
        ret = sv.DecodeMetaVal(meta_val);
        if (ret < 0) {
            //error
            return ret;
        } else if (sv.del == KEY_DELETE_MASK) {
            //deleted , reset hv
            if (sv.version == UINT16_MAX) {
                sv.version = 0;
            } else {
                sv.version = (uint16_t) (sv.version + 1);
            }
            sv.length = 0;
            sv.del = KEY_ENABLED_MASK;
        }
    }
    sv.type = DataType::SSIZE;


    int64_t incr = 0;

    std::string current;
    while (true) {
        auto n = lambda(current);
        if (n < 0) {
            return n;
        } else if (n == 0) {
            break;
        }

        std::string item_key = encode_set_key(key, current, sv.version);
        batch.Put(item_key, slice());
        incr++;
    }

    int iret = incr_ssize(ctx, key, batch, sv, meta_key, incr);
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

template<typename T>
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
            batch.Put(item_key, slice());
            change = 1;
        } else {
            int s = GetSetItemValInternal(item_key);
            if (s < 0) {
                return s;
            } else if (s == 1) {
                change = 0;
            } else {
                batch.Put(item_key, slice());
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
