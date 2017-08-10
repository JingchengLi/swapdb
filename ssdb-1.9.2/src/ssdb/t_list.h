/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_T_LIST_H
#define SSDB_T_LIST_H

#include "ssdb_impl.h"

template<typename T>
int SSDBImpl::quickList(Context &ctx, const Bytes &name, const std::string &meta_key, const std::string &meta_val,
                        T lambda) {


    leveldb::WriteBatch batch;

    int ret = 0;
    ListMetaVal lv;
    if (meta_val.size() > 0) {
        ret = lv.DecodeMetaVal(meta_val);
        if (ret < 0) {
            //error
            return ret;
        } else if (lv.del == KEY_DELETE_MASK) {
            //deleted , reset hv
            if (lv.version == UINT16_MAX) {
                lv.version = 0;
            } else {
                lv.version = (uint16_t) (lv.version + 1);
            }
            lv.del = KEY_ENABLED_MASK;
        }
    }
    lv.type = DataType::LSIZE;


    uint64_t item_seq = lv.right_seq;
    std::string item_key = encode_list_key(name, item_seq, lv.version);

    std::string current;
    while (true) {
        auto n = lambda(current);
        if (n < 0) {
            return n;
        } else if (n == 0) {
            break;
        } else if (n == 2) {
            continue;
        }

        if (item_seq < UINT64_MAX){
            item_seq += 1;
        } else{
            item_seq = 0;
        }

        update_list_key(item_key, item_seq);
        batch.Put(item_key, current);

        lv.length++;
    }

    lv.right_seq = item_seq;

    std::string new_meta_val = encode_list_meta_val(lv.length, lv.left_seq, lv.right_seq, lv.version, lv.del);
    batch.Put(meta_key, new_meta_val);


    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}






#endif //SSDB_T_LIST_H
