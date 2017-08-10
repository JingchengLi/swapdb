/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_set.h"


int SSDBImpl::incr_ssize(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const SetMetaVal &sv,
                         const std::string &meta_key, int64_t incr) {
    int ret = 1;

    if (sv.length == 0) {
        if (incr > 0) {
            std::string meta_val = encode_set_meta_val((uint64_t) incr, sv.version);
            batch.Put(meta_key, meta_val);
        } else if (incr == 0) {
            return 0;
        } else {
            return INVALID_INCR_LEN;
        }
    } else {
        uint64_t len = sv.length;
        if (incr > 0) {
            //TODO 溢出检查
            len += incr;
        } else if (incr < 0) {
            uint64_t u64 = static_cast<uint64_t>(-incr);
            if (len < u64) {
                return INVALID_INCR_LEN;
            }
            len = len - u64;
        } else {
            //incr == 0
            return ret;
        }
        if (len == 0) {
            std::string del_key = encode_delete_key(key, sv.version);
            std::string meta_val = encode_set_meta_val(sv.length, sv.version, KEY_DELETE_MASK);
            batch.Put(del_key, "");
            batch.Put(meta_key, meta_val);

            expiration->cancelExpiration(ctx, key, batch); //del expire ET key

            ret = 0;
        } else {
            std::string meta_val = encode_set_meta_val(len, sv.version);
            batch.Put(meta_key, meta_val);
        }
    }

    log_debug("incr_ssize %s %d", hexstr(key).c_str(), incr);


    return ret;
}


int SSDBImpl::sscan(Context &ctx, const Bytes &name, const Bytes &cursor, const std::string &pattern, uint64_t limit,
                    std::vector<std::string> &resp) {
    SetMetaVal sv;

    std::string meta_key = encode_meta_key(name);
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1) {
        return ret;
    }
    std::string start;
    if (cursor == "0") {
        start = encode_set_key(name, "", sv.version);
    } else {
        redisCursorService.FindElementByRedisCursor(cursor.String(), start);
    }

    Iterator *iter = this->iterator(start, "", -1);

    auto mit = std::unique_ptr<SIterator>(new SIterator(iter, name, sv.version));

//    bool end = true;
    bool end = doScanGeneric<SIterator>(mit.get(), pattern, limit, resp);

    if (!end) {
        //get new;
        uint64_t tCursor = redisCursorService.GetNewRedisCursor(iter->key().String()); //we already got it->next
        resp[1] = str(tCursor);
    }

    return 1;


}


SIterator *SSDBImpl::sscan_internal(Context &ctx, const Bytes &name, uint16_t version, const leveldb::Snapshot *snapshot) {
    leveldb::ReadOptions iterate_options(false, true);
    if (snapshot) {
        iterate_options.snapshot = snapshot;
    }

    std::string key_start;
    key_start = encode_set_key(name, "", version);

    return new SIterator(this->iterator(key_start, "", -1, iterate_options), name, version);
}

int SSDBImpl::sadd(Context &ctx, const Bytes &key, const std::set<Bytes> &mem_set, int64_t *num) {
    RecordKeyLock l(&mutex_record_, key.String());
    return saddNoLock<Bytes>(ctx, key, mem_set, num);
}

int SSDBImpl::srem(Context &ctx, const Bytes &key, const std::vector<Bytes> &members, int64_t *num) {
    RecordKeyLock l(&mutex_record_, key.String());
    leveldb::WriteBatch batch;

    int ret = 0;
    *num = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1) {
        return ret;
    }

    for (int i = 2; i < members.size(); ++i) {
        const Bytes &member = members[i];
        std::string hkey = encode_set_key(key, member, sv.version);
        int s = GetSetItemValInternal(hkey);
        if (s == 1) {
            *num += 1;
            batch.Delete(hkey);
        } else if (s < 0) {
            return s;
        }
    }

    int iret = incr_ssize(ctx, key, batch, sv, meta_key, (-1) * (*num));
    if (iret < 0) {
        return iret;
    } else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if (!s.ok()) {
        log_error("srem error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}


int SSDBImpl::scard(Context &ctx, const Bytes &key, uint64_t *llen) {
    *llen = 0;
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret < 0) {
        return ret;
    }

    *llen = sv.length;

    return ret;
}

int SSDBImpl::sismember(Context &ctx, const Bytes &key, const Bytes &member, bool *ismember) {
    SetMetaVal sv;
    std::string meta_key = encode_meta_key(key);
    int ret = GetSetMetaVal(meta_key, sv);
    if (ret < 0) {
        return ret;
    } else if (1 == ret) {
        std::string hkey = encode_set_key(key, member, sv.version);
        ret = GetSetItemValInternal(hkey);
        if (ret < 0) {
            return ret;
        } else if (ret == 0) {
            *ismember = false;
        } else {
            *ismember = true;
        }
    } else {
        //ret == 0;
        *ismember = false;
    }
    return 1;
}

int SSDBImpl::srandmember(Context &ctx, const Bytes &key, std::vector<std::string> &members, int64_t cnt) {
    leveldb::WriteBatch batch;

    const leveldb::Snapshot *snapshot = nullptr;
    SetMetaVal sv;
    int ret;
    {
        RecordKeyLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        ret = GetSetMetaVal(meta_key, sv);
        if (ret != 1) {
            return ret;
        }

        snapshot = GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<SIterator>(sscan_internal(ctx, key, sv.version, snapshot));


    bool allow_repeat = cnt < 0;
    if (allow_repeat) {
        cnt = -cnt;

        int full_repeat = int(cnt / sv.length);
        for (int j = 0; j < full_repeat; ++j) {
            auto tmp = std::unique_ptr<SIterator>(sscan_internal(ctx, key, sv.version, snapshot));
            while (tmp->next()) {
                members.emplace_back(tmp->key.String());
            }
        }

        cnt = cnt - full_repeat * sv.length;

    }


    uint64_t start_index = 0;

    if (sv.length == 0) {
        return 0;
    } else if (cnt >= sv.length) {
        cnt = sv.length;

    } else if (cnt > INT_MAX) {
        return INVALID_INT;
    } else {
        uint64_t start_index_max = sv.length - (uint64_t) cnt + 1;
        if(start_index_max > 100) start_index_max = 100;
        start_index = (uint64_t) (rand() % start_index_max);
    }


    int64_t found_cnt = 0;
    for (uint64_t i = 0; (found_cnt < cnt) && (it->next()); ++i) {
        if (i < start_index) {
            continue;
        }

        members.emplace_back(it->key.String());
        found_cnt += 1;
    }


    return 1;
}

int SSDBImpl::spop(Context &ctx, const Bytes &key, std::vector<std::string> &members, int64_t popcnt) {
    leveldb::WriteBatch batch;

    SetMetaVal sv;
    int ret;

    RecordKeyLock l(&mutex_record_, key.String());

    std::string meta_key = encode_meta_key(key);
    ret = GetSetMetaVal(meta_key, sv);
    if (ret != 1) {
        return ret;
    }

    auto it = std::unique_ptr<SIterator>(sscan_internal(ctx, key, sv.version));

    if (popcnt < 0) {
        return INDEX_OUT_OF_RANGE;
    }

    uint64_t start_index = 0;

    if (sv.length == 0) {
        return 0;
    } else if (popcnt >= sv.length) {
        popcnt = sv.length;

    } else if (popcnt > INT_MAX) {
        return INVALID_INT;
    } else {
        uint64_t start_index_max = sv.length - (uint64_t)popcnt + 1;
        if (start_index_max > 100) start_index_max = 100;
        start_index = (uint64_t) (rand() % start_index_max);
    }


    int64_t delete_cnt = 0;
    for (uint64_t i = 0; (delete_cnt < popcnt) && (it->next()); ++i) {
        if (i < start_index) {
            continue;
        }

        std::string hkey = encode_set_key(key, it->key, sv.version);
        batch.Delete(hkey);
        members.emplace_back(it->key.String());
        delete_cnt += 1;
    }

    int iret = incr_ssize(ctx, key, batch, sv, meta_key, (-1) * (delete_cnt));
    if (iret < 0) {
        return iret;
    }  else if (iret == 0) {
        ret = 0;
    } else {
        ret = 1;
    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if (!s.ok()) {
        log_error("spop error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    return ret;
}

int SSDBImpl::smembers(Context &ctx, const Bytes &key, std::vector<std::string> &members) {
    const leveldb::Snapshot *snapshot = nullptr;
    SetMetaVal sv;
    int ret;

    {
        RecordKeyLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        ret = GetSetMetaVal(meta_key, sv);
        if (ret != 1) {
            return ret;
        }

        snapshot = GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<SIterator>(sscan_internal(ctx, key, sv.version, snapshot));
    while (it->next()) {
        members.emplace_back(it->key.String());
    }

    return 1;
}

int SSDBImpl::GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv) {
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        //not found
        sv.length = 0;
        sv.del = KEY_ENABLED_MASK;
        sv.type = DataType::HSIZE;
        sv.version = 0;
        return 0;
    } else if (!s.ok() && !s.IsNotFound()) {
        //error
        log_error("GetSetMetaVal error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    } else {
        int ret = sv.DecodeMetaVal(meta_val);
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
            return 0;
        } else if (sv.type != DataType::SSIZE) {
            //error
            return WRONG_TYPE_ERR;
        }
    }
    return 1;
}

int SSDBImpl::GetSetItemValInternal(const std::string &item_key) {
    std::string val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), item_key, &val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok() && !s.IsNotFound()) {
        log_error("GetSetItemValInternal error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    return 1;
}