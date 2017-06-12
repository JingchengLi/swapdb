//
// Created by a1 on 16-11-17.
//
#include "ssdb_impl.h"


int SSDBImpl::GetListItemValInternal(const std::string &item_key, std::string *val, const leveldb::ReadOptions &options) {
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

int SSDBImpl::LIndex(Context &ctx, const Bytes &key, int64_t index, std::pair<std::string, bool> &val) {
    uint64_t sequence = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int s = GetListMetaVal(meta_key, lv);
    if (1 == s){
        sequence = getSeqByIndex(index, lv);
        std::string item_key = encode_list_key(key, sequence, lv.version);
        s = GetListItemValInternal(item_key, &val.first);
        if (s > 0) {
            val.second = true;
        }
        return s;
    }
    return s;
}

int SSDBImpl::LLen(Context &ctx, const Bytes &key, uint64_t *llen) {
    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int s = GetListMetaVal(meta_key, lv);
    if (1 == s){
        *llen = lv.length;
    }
    return s;
}

int SSDBImpl::LPop(Context &ctx, const Bytes &key, std::pair<std::string, bool> &val) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, lv);
    if (ret != 1){
        return ret;
    }

    if (lv.length == 0) {
        return 0;
    }

    ret = doListPop(ctx, key, batch, lv, meta_key, LIST_POSITION::HEAD, val);

    return ret;
}

int SSDBImpl::RPop(Context &ctx, const Bytes &key, std::pair<std::string, bool> &val) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, lv);
    if (ret != 1){
        return ret;
    }

    if (lv.length == 0) {
        return 0;
    }

    ret = doListPop(ctx, key, batch, lv, meta_key, LIST_POSITION::TAIL, val);

    return ret;
}

int SSDBImpl::doListPop(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, ListMetaVal &lv,
                        std::string &meta_key, LIST_POSITION lp, std::pair<std::string, bool> &val) {

    uint64_t item_seq;
    if (lp == LIST_POSITION::HEAD) {
        item_seq = lv.left_seq;
    } else if (lp == LIST_POSITION::TAIL){
        item_seq = lv.right_seq;
    } else {
        return -1;
    }

    std::string last_item_key = encode_list_key(key, item_seq, lv.version);
    int ret = GetListItemValInternal(last_item_key, &val.first);
    if (1 == ret) {
        val.second = true;

        uint64_t len = lv.length - 1;

        if (lp == LIST_POSITION::HEAD) {
            if (item_seq < UINT64_MAX) {
                lv.left_seq = item_seq + 1;
            } else {
                lv.left_seq = 0;
            }
        } else {
            //TAIL
            if (0 == item_seq) {
                lv.right_seq = UINT64_MAX;
            } else {
                lv.right_seq = item_seq - 1;
            }
        }

        if (0 == len) {
            batch.Delete(last_item_key);
            std::string del_key = encode_delete_key(key, lv.version);
            std::string size_val = encode_list_meta_val(lv.length, 0, 0, lv.version, KEY_DELETE_MASK);
            batch.Put(meta_key, size_val);
            batch.Put(del_key, "");
            expiration->cancelExpiration(ctx, key, batch); //del expire ET key
            ret = 0;
        } else {
            lv.length = len;
            std::string new_meta_val = EncodeValueListMeta(lv);

            batch.Put(meta_key, new_meta_val);
            batch.Delete(last_item_key);
        }

        leveldb::Status s = CommitBatch(ctx, &(batch));
        if(!s.ok()){
            log_error("error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }
    }
    return ret;
}


int SSDBImpl::LPushX(Context &ctx, const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, lv);
    if (ret < 0){
        return ret;
    } else if (ret == 0){
        *llen = 0;
        return 0;
    }

    ret = doListPush<Bytes>(ctx, key, batch, val, offset, meta_key, lv, LIST_POSITION::HEAD);
    *llen = lv.length;

    return ret;
}

int SSDBImpl::LPush(Context &ctx, const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {

    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, lv);
    if (ret < 0){
        return ret;
    }

    ret = doListPush<Bytes>(ctx, key, batch, val, offset, meta_key, lv, LIST_POSITION::HEAD);
    *llen = lv.length;

    return ret;
}

template <typename T>
int SSDBImpl::doListPush(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const std::vector<T> &val, int offset, std::string &meta_key,
                      ListMetaVal &meta_val, LIST_POSITION lp) {

    int size = (int)val.size();
    int num  = size - offset;
    if (num <= 0){
        return -1;
    }
    if (meta_val.length > (UINT64_MAX-num)){
        log_fatal("list reach the max length.");
        return -1;
    }

    uint64_t len = meta_val.length + num;
    uint64_t item_seq = 0;

    if (lp == LIST_POSITION::HEAD) {
        item_seq = meta_val.left_seq;
    } else {
        //TAIL
        item_seq = meta_val.right_seq;
    }

    for (int i = offset; i < size; ++i) {
        if (lp == LIST_POSITION::HEAD) {
            if (item_seq > 0) {
                item_seq -= 1;
            } else {
                item_seq = UINT64_MAX;
            }
        } else {
            //TAIL
            if (item_seq < UINT64_MAX){
                item_seq += 1;
            } else{
                item_seq = 0;
            }

        }

        std::string item_key = encode_list_key(key, item_seq, meta_val.version);
        batch.Put(item_key, leveldb::Slice(val[i].data(), val[i].size()));
    }

    meta_val.length = len;
    if (lp == LIST_POSITION::HEAD) {
        meta_val.left_seq = item_seq;
    } else {
        //TAIL
        meta_val.right_seq = item_seq;
    }
    std::string new_meta_val = EncodeValueListMeta(meta_val);
    batch.Put(meta_key, new_meta_val);


    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return 1;
}


int SSDBImpl::RPushX(Context &ctx, const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, lv);
    if (ret < 0){
        return ret;
    } else if (ret == 0){
        *llen = 0;
        return 0;
    }

    ret = doListPush<Bytes>(ctx, key, batch, val, offset, meta_key, lv, LIST_POSITION::TAIL);
    *llen = lv.length;

    return ret;
}

int SSDBImpl::RPush(Context &ctx, const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    *llen = 0;
    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);

    int ret = GetListMetaVal(meta_key, lv);
    if (ret < 0){
        return ret;
    }

    ret = doListPush<Bytes>(ctx, key, batch, val, offset, meta_key, lv, LIST_POSITION::TAIL);
    *llen = lv.length;

    return ret;
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
        log_error("error: %s", s.ToString().c_str());
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


int SSDBImpl::LSet(Context &ctx, const Bytes &key, int64_t index, const Bytes &val) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetListMetaVal(meta_key, lv);
    if (ret != 1) {
        return ret;
    }

    int64_t max_negative_index, max_index;
    if (lv.length < UINT64_MAX/2) {
        max_negative_index = - lv.length;
        max_index = lv.length - 1;
    } else {
        max_negative_index = INT64_MIN;
        max_index = INT64_MAX;
    }

    if (index > max_index || index < max_negative_index) {
//        log_fatal("invalid list index.");
        return INDEX_OUT_OF_RANGE;
    }

    uint64_t seq = getSeqByIndex(index, lv);
    std::string item_key = encode_list_key(key, seq, lv.version);
    batch.Put(item_key, leveldb::Slice(val.data(),val.size()));

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;
}

int SSDBImpl::ltrim(Context &ctx, const Bytes &key, int64_t start, int64_t end) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;


    int ret;

    ListMetaVal lv;
    std::string meta_key = encode_meta_key(key);
    ret = GetListMetaVal(meta_key, lv);
    if (ret != 1){
        return ret;
    }

    uint64_t left_seq = lv.left_seq;
    uint64_t right_seq = lv.right_seq;

    int64_t llen = (int64_t)lv.length;
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;


    uint64_t ltrim = 0;
    uint64_t rtrim = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        ltrim = lv.length;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        uint64_t begin_seq = getSeqByIndex(start, lv);
        uint64_t end_seq = getSeqByIndex(end, lv);

        ltrim = begin_seq - left_seq;
        rtrim = right_seq - end_seq;
    }


    uint64_t deleted = 0;
    while (ltrim--){
        std::string val;
        std::string item_key = encode_list_key(key, lv.left_seq, lv.version);
        batch.Delete(item_key);
        deleted ++;

        if (UINT64_MAX == lv.left_seq) {
            lv.left_seq = 0;
        } else {
            lv.left_seq++;
        }
    }

    while (rtrim--){
        std::string val;
        std::string item_key = encode_list_key(key, lv.right_seq, lv.version);
        batch.Delete(item_key);
        deleted ++;

        if (0 == lv.right_seq) {
            lv.right_seq = UINT64_MAX;
        } else {
            lv.right_seq--;
        }
    }

    lv.length =  lv.length - deleted;


    if (0 ==  lv.length) {
        std::string del_key = encode_delete_key(key, lv.version);
        std::string size_val = encode_list_meta_val(lv.length, 0, 0, lv.version, KEY_DELETE_MASK);
        batch.Put(meta_key, size_val);
        batch.Put(del_key, "");
        expiration->cancelExpiration(ctx, key, batch); //del expire ET key
        ret = 0;
    } else {
        std::string new_meta_val = EncodeValueListMeta(lv);
        batch.Put(meta_key, new_meta_val);
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if(!s.ok()){
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}


int SSDBImpl::lrange(Context &ctx, const Bytes &key, int64_t start, int64_t end, std::vector<std::string> &list){

    int ret;

    ListMetaVal lv;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordKeyLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        ret = GetListMetaVal(meta_key, lv);
        if (ret != 1){
            return ret;
        }

        snapshot = GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    leveldb::ReadOptions readOptions = leveldb::ReadOptions();
    readOptions.fill_cache = true;
    readOptions.snapshot = snapshot;


    int64_t llen = (int64_t)lv.length;
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        return 1;
    }
    if (end >= llen) end = llen-1;
    int64_t rangelen = (end-start)+1;

    uint64_t begin_seq = getSeqByIndex(start, lv);
    uint64_t cur_seq = begin_seq;

    while (rangelen--){
        std::string val;
        std::string item_key = encode_list_key(key, cur_seq, lv.version);
        ret = GetListItemValInternal(item_key, &val, readOptions);
        if (1 != ret){
            list.clear();
            return -1;
        }
        list.push_back(val);

        if (UINT64_MAX == cur_seq) {
            cur_seq = 0;
        } else {
            cur_seq++;
        }
    }

    return 1;
}