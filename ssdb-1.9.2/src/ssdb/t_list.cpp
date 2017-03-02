//
// Created by a1 on 16-11-17.
//
#include "ssdb_impl.h"



int SSDBImpl::GetListItemVal(const std::string &item_key, std::string *val, const leveldb::ReadOptions& options) {
    leveldb::Status s = ldb->Get(options, item_key, val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        return -1;
    }
    return 1;
}

static std::string EncodeValueListMeta(const ListMetaVal &meta_val);

std::string EncodeValueListMeta(const ListMetaVal &meta_val) {
    return encode_list_meta_val(meta_val.length, meta_val.left_seq, meta_val.right_seq, meta_val.version, meta_val.del);
}

uint64_t getSeqByIndex(int64_t index, const ListMetaVal &meta_val) {
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
        std::string item_key = encode_list_key(key, sequence, lv.version);
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
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (1 == ret) {
        if (meta_val.length > 0) {
            ret = doListPop(batch, meta_val, key, meta_key, LIST_POSTION::HEAD, val);
            if (1 == ret){
                leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
                if(!s.ok()){
                    return STORAGE_ERR;
                }
            }
        } else {
            return 0;
        }
    }
    return ret;
}

int SSDBImpl::RPop(const Bytes &key, std::string *val) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, meta_val);
    if (1 == ret) {
        if (meta_val.length > 0) {
            ret = doListPop(batch, meta_val, key, meta_key, LIST_POSTION::TAIL, val);
            if (1 == ret){
                leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
                if(!s.ok()){
                    return STORAGE_ERR;
                }
            }
        } else {
            return 0;
        }
    }
    return ret;
}

int SSDBImpl::doListPop(leveldb::WriteBatch &batch, ListMetaVal &meta_val,
                        const Bytes &key, std::string &meta_key, LIST_POSTION lp, std::string *val) {

    uint64_t item_seq;
    if (lp == LIST_POSTION::HEAD) {
        item_seq = meta_val.left_seq;
    } else if (lp == LIST_POSTION::TAIL){
        item_seq = meta_val.right_seq;
    } else {
        return -1;
    }

    std::string last_item_key = encode_list_key(key, item_seq, meta_val.version);
    int ret = GetListItemVal(last_item_key, val);
    if (1 == ret) {
        uint64_t len = meta_val.length - 1;

        if (lp == LIST_POSTION::HEAD) {
            if (item_seq < UINT64_MAX) {
                meta_val.left_seq = item_seq + 1;
            } else {
                meta_val.left_seq = 0;
            }
        } else {
            //TAIL
            if (0 == item_seq) {
                meta_val.right_seq = UINT64_MAX;
            } else {
                meta_val.right_seq = item_seq - 1;
            }
        }

        if (0 == len) {
            batch.Delete(last_item_key);
            std::string del_key = encode_delete_key(key, meta_val.version);
            std::string size_val = encode_list_meta_val(meta_val.length, 0, 0, meta_val.version, KEY_DELETE_MASK);
            batch.Put(meta_key, size_val);
            batch.Put(del_key, "");
            this->edel_one(batch, key); //del expire ET key
        } else {
            meta_val.length = len;
            std::string new_meta_val = EncodeValueListMeta(meta_val);

            batch.Put(meta_key, new_meta_val);
            batch.Delete(last_item_key);
        }
    }
    return ret;
}


int SSDBImpl::LPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret < 0){
        return ret;
    } else if (ret == 0){
        *llen = 0;
        return 1;
    }

    ret = DoLPush(batch, key, val, offset, meta_key, meta_val);
    *llen = meta_val.length;

    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::LPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {

    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret < 0){
        return ret;
    }

    ret = DoLPush(batch, key, val, offset, meta_key, meta_val);
    *llen = meta_val.length;

    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}


int SSDBImpl::DoLPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key,
                      ListMetaVal &meta_val) {

    uint64_t len = meta_val.length;
    uint64_t left_seq = meta_val.left_seq;

    int size = (int)val.size();
    int num  = size - offset;
    if (num <= 0){
        return 0;
    }

    if (meta_val.length < UINT64_MAX-(num)){
        len = meta_val.length + num;
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
        batch.Put(item_key, val[i].String());
    }

    meta_val.length = len;
    meta_val.left_seq = left_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);
    batch.Put(meta_key, new_meta_val);

    return size-offset;
}

int SSDBImpl::DoRPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key,
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
        batch.Put(item_key, leveldb::Slice(val[i].data(), val[i].size()));
    }

    meta_val.right_seq = right_seq;
    std::string new_meta_val = EncodeValueListMeta(meta_val);
    batch.Put(meta_key, new_meta_val);

    return num;
}

int SSDBImpl::RPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret < 0){
        return ret;
    } else if (ret == 0){
        *llen = 0;
        return 1;
    }

    ret = DoRPush(batch, key, val, offset, meta_key, meta_val);
    *llen = meta_val.length;

    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::RPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret < 0){
        return ret;
    }

    ret = DoRPush(batch, key, val, offset, meta_key, meta_val);
    *llen = meta_val.length;

    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return 1;
}

int SSDBImpl::GetListMetaVal(const std::string &meta_key, ListMetaVal &lv) {

    int ret = 0;

    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        lv.left_seq = 0;
        lv.right_seq = UINT64_MAX;
        lv.length = 0;
        lv.del = KEY_ENABLED_MASK;
        lv.version = 0;
        return 0;
    } else if (!s.ok() && !s.IsNotFound()){
        //error
        return STORAGE_ERR;
    } else{
        ret = lv.DecodeMetaVal(meta_val);
        if (ret < 0){
            //error
            return ret;
        } else if (lv.del == KEY_DELETE_MASK){
            if (lv.version == UINT16_MAX){
                lv.version = 0;
            } else{
                lv.version = (uint16_t)(lv.version+1);
            }
            lv.left_seq = 0;
            lv.right_seq = UINT64_MAX;
            lv.length = 0;
            lv.del = KEY_ENABLED_MASK;
            return 0;
        } else if (lv.type != DataType::LSIZE){
            return WRONG_TYPE_ERR;
        }

    }
    return 1;
}

int SSDBImpl::rpushNoLock(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, meta_val);
    if (ret < 0){
        return ret;
    }

    ret = DoRPush(batch, key, val, offset, meta_key, meta_val);
    *llen = meta_val.length;

    if (ret <= 0){
        return ret;
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return -1;
    }
    return 1;
}

int SSDBImpl::LSet(const Bytes &key, const int64_t index, const Bytes &val) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

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
//            log_fatal("invalid list index.");
            return INDEX_OUT_OF_RANGE;
        }

        uint64_t seq = getSeqByIndex(index, meta_val);
        std::string item_key = encode_list_key(key, seq, meta_val.version);
        batch.Put(item_key, val.String());

        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if(!s.ok()){
            return STORAGE_ERR;
        }
    }
    return ret;
}

int SSDBImpl::ltrim(const Bytes &key, int64_t start, int64_t end) {
    RecordLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;


    int ret;

    ListMetaVal meta_val;
    std::string meta_key = encode_meta_key(key);
    ret = GetListMetaVal(meta_key, meta_val);
    if (ret != 1){
        return ret;
    }

    uint64_t left_seq = meta_val.left_seq;
    uint64_t right_seq = meta_val.right_seq;

    int64_t llen = (int64_t)meta_val.length;
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;


    uint64_t ltrim = 0;
    uint64_t rtrim = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        ltrim = meta_val.length;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        uint64_t begin_seq = getSeqByIndex(start, meta_val);
        uint64_t end_seq = getSeqByIndex(end, meta_val);

        ltrim = begin_seq - left_seq;
        rtrim = right_seq - end_seq;
    }


    uint64_t deleted = 0;
    while (ltrim--){
        std::string val;
        std::string item_key = encode_list_key(key, meta_val.left_seq, meta_val.version);
        batch.Delete(item_key);
        deleted ++;

        if (UINT64_MAX == meta_val.left_seq) {
            meta_val.left_seq = 0;
        } else {
            meta_val.left_seq++;
        }
    }

    while (rtrim--){
        std::string val;
        std::string item_key = encode_list_key(key, meta_val.right_seq, meta_val.version);
        batch.Delete(item_key);
        deleted ++;

        if (0 == meta_val.right_seq) {
            meta_val.right_seq = UINT64_MAX;
        } else {
            meta_val.right_seq--;
        }
    }

    meta_val.length =  meta_val.length - deleted;


    if (0 ==  meta_val.length) {
        std::string del_key = encode_delete_key(key, meta_val.version);
        std::string size_val = encode_list_meta_val(meta_val.length, 0, 0, meta_val.version, KEY_DELETE_MASK);
        batch.Put(meta_key, size_val);
        batch.Put(del_key, "");
        this->edel_one(batch, key); //del expire ET key
    } else {
        std::string new_meta_val = EncodeValueListMeta(meta_val);
        batch.Put(meta_key, new_meta_val);
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if(!s.ok()){
        return STORAGE_ERR;
    }

    return ret;
}


int SSDBImpl::lrange(const Bytes &key, int64_t start, int64_t end, std::vector<std::string> *list){

    int ret;

    ListMetaVal meta_val;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        ret = GetListMetaVal(meta_key, meta_val);
        if (ret != 1){
            return ret;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    leveldb::ReadOptions readOptions = leveldb::ReadOptions();
    readOptions.fill_cache = false;
    readOptions.snapshot = snapshot;


    int64_t llen = (int64_t)meta_val.length;
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        return 0;
    }
    if (end >= llen) end = llen-1;
    int64_t rangelen = (end-start)+1;

    uint64_t begin_seq = getSeqByIndex(start, meta_val);
//  uint64_t end_seq = getSeqByIndex(end, meta_val);
    uint64_t cur_seq = begin_seq;

    while (rangelen--){
        std::string val;
        std::string item_key = encode_list_key(key, cur_seq, meta_val.version);
        ret = GetListItemVal(item_key, &val, readOptions);
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