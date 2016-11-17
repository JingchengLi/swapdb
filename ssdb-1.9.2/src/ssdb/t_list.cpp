//
// Created by a1 on 16-11-17.
//
#include "ssdb_impl.h"

int SSDBImpl::GetListMetaVal(const std::string &meta_key, ListMetaVal &lv) {
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        return -1;
    } else{
        int ret = lv.DecodeMetaVal(meta_val);
        if (ret == -1){
            return -1;
        } else if (lv.del == KEY_DELETE_MASK){
            return 0;
        }
    }
    return 1;
}

int SSDBImpl::GetListItemVal(const std::string &item_key, std::string *val) {
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), item_key, val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        return -1;
    }
    return 1;
}

static std::string EncodeValueListMeta(const ListMetaVal &meta_val) {
    return encode_list_meta_val(meta_val.length, meta_val.left_seq, meta_val.right_seq, meta_val.version, meta_val.del);
}

static uint64_t getSeqByIndex(int64_t index, const ListMetaVal &meta_val) {
    uint64_t seq;
    if (meta_val.left_seq > meta_val.right_seq) {
        if (index >= 0) {
            uint64_t u64 = static_cast<uint64_t>(index);
            if (UINT64_MAX - meta_val.left_seq >= u64) {
                seq = meta_val.left_seq + u64;
            } else {
                seq = u64 + meta_val.left_seq - UINT64_MAX - 1;
            }
        } else {
            uint64_t u64 = static_cast<uint64_t>(-index - 1);
            if (meta_val.right_seq >= u64) {
                seq = meta_val.right_seq - u64;
            } else {
                seq = UINT64_MAX - u64 + meta_val.right_seq + 1;
            }
        }
    } else {
        if (index >= 0) {
            uint64_t u64 = static_cast<uint64_t>(index);
            seq = meta_val.left_seq + u64;
        } else {
            uint64_t u64 = static_cast<uint64_t>(-index - 1);
            seq = meta_val.right_seq - u64;
        }
    }
    return seq;
}

int SSDBImpl::LIndex(const Bytes &key, const int64_t index, std::string *val) {
    uint64_t sequence = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int s = GetListMetaVal(meta_key, lv);
    if (1 == s){
        sequence = getSeqByIndex(index, lv);
        std::string item_key = encode_list_key(key.String(), sequence, lv.version);
        s = GetListItemVal(item_key, val);
        return s;
    }
    return s;
}

int SSDBImpl::LLen(const Bytes &key, uint64_t *llen) {
    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int s = GetListMetaVal(meta_key, lv);
    if (1 == s){
        *llen = lv.length;
    }
    return s;
}

int SSDBImpl::LPop(const Bytes &key, std::string *val) {
    Transaction trans(binlogs);

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (1 == ret) {
        if (meta_val.length > 0) {
            uint64_t first_item_seq = meta_val.left_seq;
            std::string first_item_key = encode_list_key(key.String(), first_item_seq, meta_val.version);
            ret = GetListItemVal(first_item_key, val);
            if (1 == ret) {
                uint64_t len = meta_val.length - 1;
                uint64_t left_seq;
                if (first_item_seq < UINT64_MAX) {
                    left_seq = first_item_seq + 1;
                } else {
                    left_seq = 0;
                }

                if (0 == len) {
                    binlogs->Delete(first_item_key);
                    binlogs->Delete(meta_key);
                } else {
                    meta_val.length = len;
                    meta_val.left_seq = left_seq;
                    std::string new_meta_val = EncodeValueListMeta(meta_val);

                    binlogs->Delete(first_item_key);
                    binlogs->Put(meta_key, new_meta_val);
                }

                leveldb::Status s = binlogs->commit();
                if(!s.ok()){
                    return -1;
                }
            }
        } else {
            return 0;
        }
    }
    return ret;
}

int SSDBImpl::DoLPush(ListMetaVal &meta_val, const Bytes &key, const Bytes &val, std::string &meta_key) {
    uint64_t len, left_seq;
    if (meta_val.length < UINT64_MAX) {
        len = meta_val.length + 1;
    } else {
        log_fatal("list reach the max length.");
        return -1;
    }
    if (meta_val.left_seq > 0) {
        left_seq = meta_val.left_seq - 1;
    } else {
        left_seq = UINT64_MAX;
    }
    meta_val.length = len;
    meta_val.left_seq = left_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);

    std::string item_key = encode_list_key(key, left_seq, meta_val.version);
    binlogs->Put(meta_key, new_meta_val);
    binlogs->Put(item_key, val.String());

    return 1;
}

void SSDBImpl::PushFirstListItem(const Bytes &key, const Bytes &val, const std::string &meta_key, uint16_t version) {
    std::string new_meta_val = encode_list_meta_val(1, 0, 0, version);

    std::string item_key = encode_list_key(key, 0, version);

    binlogs->Put(meta_key, new_meta_val);
    binlogs->Put(item_key, val.String());
}

int  SSDBImpl::LPush(const Bytes &key, const Bytes &val, uint64_t *llen) {
    Transaction trans(binlogs);

    uint64_t old_len = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (-1 == ret){
        return -1;
    } else if (1 == ret) {
        old_len = meta_val.length;
        ret = DoLPush(meta_val, key, val, meta_key);
        if (-1 == ret){
            return -1;
        }
    } else if (0 == ret && meta_val.del == KEY_DELETE_MASK) {
        PushFirstListItem(key, val, meta_key, (uint16_t)(meta_val.version+1));
    } else if(0 == ret){
        PushFirstListItem(key, val, meta_key, 0);
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        return -1;
    }
    *llen = old_len + 1;
    return 1;
}
