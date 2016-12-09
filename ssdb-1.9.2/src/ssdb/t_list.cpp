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
        } else if (lv.type != DataType::LSIZE){
            return -1;
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
                    std::string del_key = encode_delete_key(key, meta_val.version);
                    std::string size_val = encode_list_meta_val(meta_val.length, 0, 0, meta_val.version, KEY_DELETE_MASK);
                    binlogs->Put(meta_key, size_val);
                    binlogs->Put(del_key, "");
                    this->edel_one(key); //del expire ET key
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

int SSDBImpl::DoLPush(ListMetaVal &meta_val, const Bytes &key, const std::vector<Bytes> &val, int offset,
                      std::string &meta_key) {
    uint64_t len;
    uint64_t left_seq = meta_val.left_seq;
    int size = (int)val.size();
    if (meta_val.length < UINT64_MAX-(size-offset)){
        len = meta_val.length + size - offset;
    } else {
        log_fatal("list reach the max length.");
        return -1;
    }

    for (int i = offset; i < size; ++i) {
        if (left_seq > 0) {
            left_seq -= 1;
        } else {
            left_seq = UINT64_MAX;
        }
        std::string item_key = encode_list_key(key, left_seq, meta_val.version);
        binlogs->Put(item_key, val[i].String());
    }

    meta_val.length = len;
    meta_val.left_seq = left_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);
    binlogs->Put(meta_key, new_meta_val);

    return size-offset;
}

void SSDBImpl::PushFirstListItem(const Bytes &key, const Bytes &val, const std::string &meta_key, uint16_t version) {
    std::string new_meta_val = encode_list_meta_val(1, 0, 0, version);

    std::string item_key = encode_list_key(key, 0, version);

    binlogs->Put(meta_key, new_meta_val);
    binlogs->Put(item_key, val.String());
}

int SSDBImpl::DoFirstLPush(const Bytes &key, const std::vector<Bytes> &val, int offset, const std::string &meta_key,
                           uint16_t version) {
    uint64_t left_seq = 1;
    int size = (int)val.size();
    for (int i = offset; i < size; ++i) {
        if (left_seq > 0) {
            left_seq -= 1;
        } else {
            left_seq = UINT64_MAX;
        }
        std::string item_key = encode_list_key(key, left_seq, version);
        binlogs->Put(item_key, val[i].String());
    }

    if (size - offset > 0){
        std::string meta_val = encode_list_meta_val((uint64_t)(size-offset), left_seq, 0, version);
        binlogs->Put(meta_key, meta_val);
    }

    return size-offset;
}

int SSDBImpl::LPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    Transaction trans(binlogs);

    uint64_t old_len = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (-1 == ret){
        return -1;
    } else if (1 == ret) {
        old_len = meta_val.length;
        ret = DoLPush(meta_val, key, val, offset, meta_key);
        if (-1 == ret){
            return -1;
        }
    } else if (0 == ret && meta_val.del == KEY_DELETE_MASK) {
        uint16_t version;
        if (meta_val.version == UINT16_MAX){
            version = 0;
        } else{
            version = (uint16_t)(meta_val.version+1);
        }
        ret = DoFirstLPush(key, val, offset, meta_key, version);
    } else if(0 == ret){
        ret = DoFirstLPush(key, val, offset, meta_key, 0);
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        return -1;
    }
    *llen = old_len + ret;
    return 1;
}

int SSDBImpl::RPop(const Bytes &key, std::string *val) {
    Transaction trans(binlogs);

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (1 == ret) {
        if (meta_val.length > 0) {
            ret = DoRPop(meta_val, key, meta_key, val);
            if (1 == ret){
                leveldb::Status s = binlogs->commit();
                if(!s.ok()){
                    log_debug("RPOP error!");
                    return -1;
                }
            }
        } else {
            return 0;
        }
    }
    return ret;
}

int SSDBImpl::DoRPop(ListMetaVal &meta_val, const Bytes &key, std::string &meta_key, std::string *val) {
    uint64_t last_item_seq = meta_val.right_seq;
    std::string last_item_key = encode_list_key(key, last_item_seq, meta_val.version);
    int ret = GetListItemVal(last_item_key, val);
    if (1 == ret) {
        uint64_t len = meta_val.length - 1;
        uint64_t right_seq;
        if (0 == last_item_seq) {
            right_seq = UINT64_MAX;
        } else {
            right_seq = last_item_seq - 1;
        }

        if (0 == len) {
            binlogs->Delete(last_item_key);
//            binlogs->Delete(meta_key);
            std::string del_key = encode_delete_key(key, meta_val.version);
            std::string size_val = encode_list_meta_val(meta_val.length, 0, 0, meta_val.version, KEY_DELETE_MASK);
            binlogs->Put(meta_key, size_val);
            binlogs->Put(del_key, "");
            this->edel_one(key); //del expire ET key
        } else {
            meta_val.length = len;
            meta_val.right_seq = right_seq;
            std::string new_meta_val = EncodeValueListMeta(meta_val);

            binlogs->Put(meta_key, new_meta_val);
            binlogs->Delete(last_item_key);
        }
    }
    return ret;
}

int SSDBImpl::DoRPush(ListMetaVal &meta_val, const Bytes &key, const Bytes &val, std::string &meta_key) {
    uint64_t len, right_seq;
    if (meta_val.length < UINT64_MAX) {
        len = meta_val.length + 1;
    } else {
        log_fatal("list reach the max length.");
        return -1;
    }

    if (meta_val.right_seq < UINT64_MAX) {
        right_seq = meta_val.right_seq + 1;
    } else {
        right_seq = 0;
    }

    meta_val.length = len;
    meta_val.right_seq = right_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);

    std::string item_key = encode_list_key(key, right_seq, meta_val.version);
    binlogs->Put(meta_key, new_meta_val);
    binlogs->Put(item_key, val.String());

    return 1;
}

int SSDBImpl::DoRPush(const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key,
                      ListMetaVal &meta_val) {
    int size = (int)val.size();
    int num  = size - offset;
    if (num <= 0){
        return 0;
    }
    uint64_t right_seq = meta_val.right_seq;
    if (meta_val.length < UINT64_MAX - num){
        meta_val.length += num;
    } else {
        log_fatal("list reach the max length.");
        return -1;
    }

    for (int i = offset; i < size; ++i) {
        if (right_seq < UINT64_MAX){
            right_seq += 1;
        } else{
            right_seq = 0;
        }
        std::string item_key = encode_list_key(key, right_seq, meta_val.version);
        binlogs->Put(item_key, val[i].String());
    }

    meta_val.right_seq = right_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);
    binlogs->Put(meta_key, new_meta_val);

    return num;
}

int SSDBImpl::RPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    Transaction trans(binlogs);

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (-1 == ret){
        return -1;
    } else if (1 == ret) {
        *llen = meta_val.length;
    }  else if (0 == ret && meta_val.del == KEY_DELETE_MASK) {
        meta_val.left_seq = 0;
        meta_val.right_seq = UINT64_MAX;
        meta_val.length = 0;
        meta_val.del = KEY_ENABLED_MASK;
        if (meta_val.version == UINT16_MAX){
            meta_val.version = 0;
        } else{
            meta_val.version = (uint16_t)(meta_val.version+1);
        }
    } else if(0 == ret){
        meta_val.left_seq = 0;
        meta_val.right_seq = UINT64_MAX;
        meta_val.length = 0;
        meta_val.del = KEY_ENABLED_MASK;
        meta_val.version = 0;
    }

    ret = DoRPush(key, val, offset, meta_key, meta_val);
    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()){
        return -1;
    }
    *llen = meta_val.length;
    return 1;
}

int SSDBImpl::LSet(const Bytes &key, const int64_t index, const Bytes &val) {
    Transaction trans(binlogs);

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (1 == ret) {
        int64_t max_negative_index, max_index;
        if (meta_val.length < UINT64_MAX/2) {
            max_negative_index = - meta_val.length;
            max_index = meta_val.length - 1;
        } else {
            max_negative_index = INT64_MIN;
            max_index = INT64_MAX;
        }

        if (index > max_index || index < max_negative_index) {
            log_fatal("invalid list index.");
            return -1;
        }

        uint64_t seq = getSeqByIndex(index, meta_val);
        std::string item_key = encode_list_key(key, seq, meta_val.version);
        binlogs->Put(item_key, val.String());

        leveldb::Status s = binlogs->commit();
        if(!s.ok()){
            return -1;
        }
    }
    return ret;
}

int SSDBImpl::lrange(const Bytes &key, int64_t start, int64_t end, std::vector<std::string> *list){
    Transaction trans(binlogs);

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret != 1){
        return ret;
    }
    int64_t llen = (int64_t)meta_val.length;
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        return -1;
    }
    if (end >= llen) end = llen-1;
    int64_t rangelen = (end-start)+1;

    uint64_t begin_seq = getSeqByIndex(start, meta_val);
//  uint64_t end_seq = getSeqByIndex(end, meta_val);
    uint64_t cur_seq = begin_seq;

    while (rangelen--){
        std::string val;
        std::string item_key = encode_list_key(key, cur_seq, meta_val.version);
        ret = GetListItemVal(item_key, &val);
        if (1 != ret){
            list->clear();
            return -1;
        }
        list->push_back(val);

        if (UINT64_MAX == cur_seq) {
            cur_seq = 0;
        } else {
            cur_seq++;
        }
    }

    return ret;
}

int64_t SSDBImpl::LDelKeyNoLock(const Bytes &name, char log_type) {
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetListMetaVal(meta_key, lv);
    if (ret != 1){
        return ret;
    }

    if (lv.length > MAX_NUM_DELETE){
        std::string del_key = encode_delete_key(name, lv.version);
        std::string meta_val = encode_list_meta_val(lv.length, lv.version, KEY_DELETE_MASK);
        binlogs->Put(del_key, "");
        binlogs->Put(meta_key, meta_val);
        return lv.length;
    }

    std::string key_start = encode_list_key(name, 0, lv.version);
    LIterator *it = new LIterator(this->iterator(key_start, "", -1), name, lv.version);
    int num = 0;
    while (it->next()){
        std::string item_key = encode_list_key(it->name, it->seq, it->version);
        binlogs->Delete(item_key);
        num++;
    }
    delete it;
    it = NULL;

    if (num == lv.length){
        binlogs->Delete(meta_key);
    } else{
        return -1;
    }
    return num;
}
