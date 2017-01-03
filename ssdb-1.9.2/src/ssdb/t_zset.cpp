/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <limits.h>
#include "../include.h"
#include "ssdb_impl.h"


static int zset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, double score);

static int zdel_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key);

static int incr_zsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, int64_t incr);

void zset_internal(const SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, double new_score,
                   uint16_t cur_version);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::zset(const Bytes &name, const Bytes &key, const Bytes &score) {
    RecordLock l(&mutex_record_, name.String());
    leveldb::WriteBatch batch;

    double new_score = score.Double();

    if (new_score > ZSET_SCORE_MAX || new_score < ZSET_SCORE_MIN) {
        return -1;
    }

    int ret = zset_one(this, batch, name, key, new_score);
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, batch, name, ret) == -1) {
                log_error("incr_zsize error");

                return -1;
            }
        }
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if (!s.ok()) {
            log_error("zset error: %s", s.ToString().c_str());
            return -1;
        }
    }
    return ret;
}


int SSDBImpl::multi_zset(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags) {
    RecordLock l(&mutex_record_, name.String());
    return zsetNoLock(name, sortedSet, flags);
}



int zsetAdd(SSDBImpl *ssdb, leveldb::WriteBatch &batch, bool needCheck,
             const Bytes &name, const Bytes &key, double score, uint16_t cur_version,
            int *flags, double *newscore) {
    /* Turn options into simple to check vars. */
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */

    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return -1;
    }

    if (needCheck) {
        double old_score = 0;

        int found = ssdb->zget(name, key, &old_score);
        if (found == -1) {
            return -1;
        }

        if (found == 0) {
            if (!xx) {
                if (newscore) *newscore = score;
                zset_internal(ssdb, batch, name, key, score, cur_version);
                *flags |= ZADD_ADDED;
            } else {
                *flags |= ZADD_NOP;
            }

            return 1;

        } else if (found == 1) {
            if (nx) {
                *flags |= ZADD_NOP;
                return 1;
            }

            if (incr) {
                score += old_score;
                if (isnan(score)) {
                    *flags |= ZADD_NAN;
                    return -1;
                }
                if (newscore) *newscore = score;
            }

            if (fabs(old_score - score) < eps) {
                //same
            } else {
                if (newscore) *newscore = score;

                string old_score_key = encode_zscore_key(name, key, old_score, cur_version);
                batch.Delete(old_score_key);
                zset_internal(ssdb, batch, name, key, score, cur_version);
                *flags |= ZADD_UPDATED;
            }
            return 1;
        } else {
            //error
            return -1;
        }
    } else {

        if (!xx) {
            if (newscore) *newscore = score;
            zset_internal(ssdb, batch, name, key, score, cur_version);
            *flags |= ZADD_ADDED;
        } else {
            *flags |= ZADD_NOP;
        }

        return 1;
    }
}


int SSDBImpl::zsetNoLock(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags) {
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    /* XX and NX options at the same time are not compatible. */
    if (nx && xx) {
        return -1;
    }

    //INCR option supports a single increment-element pair
    if (incr && sortedSet.size() > 1) {
        return -1;
    }

    /* The following vars are used in order to track what the command actually
 * did during the execution, to reply to the client and to trigger the
 * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */


    leveldb::WriteBatch batch;

    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetZSetMetaVal(meta_key, zv);
    uint16_t cur_version = 0;
    bool needCheck = false;

    if (ret == -1) {
        return -1;
    } else if (ret == 0) {
        needCheck = false;
        if (zv.del == KEY_DELETE_MASK) {
            if (zv.version == UINT16_MAX){
                cur_version = 0;
            } else{
                cur_version = (uint16_t) (zv.version + 1);
            }
        } else {
            cur_version = 0;

        }
    } else {
        needCheck = true;
        cur_version = zv.version;
    }

    double newscore;

    for(auto const &it : sortedSet)
    {
        const Bytes &key = it.first;
        const Bytes &val = it.second;

//        log_info("%s:%s" , hexmem(key.data(),key.size()).c_str(), hexmem(val.data(),val.size()).c_str());

        double score = val.Double();

        if (score > ZSET_SCORE_MAX || score < ZSET_SCORE_MIN) {
            return -1;
        }
        int retflags = flags;

        int retval = zsetAdd(this, batch ,needCheck, name, key, score, cur_version, &retflags, &newscore);
        if (retval == -1) {
            return -1;
        }

        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;

    }


    if (added > 0) {
        if (incr_zsize(this, batch, name, added) == -1) {
            log_error("incr_zsize error");
            return -1;
        }
    }

    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
    if (!s.ok()) {
        log_error("zset error: %s", s.ToString().c_str());
        return -1;
    }

    if (ch) {
        return added+updated;
    } else {
        return added;
    }
}


int64_t SSDBImpl::zcount(const Bytes &name, const Bytes &score_start, const Bytes &score_end) {
    int64_t count = 0;

    ZSetMetaVal zv;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock l(&mutex_record_, name.String());
        std::string meta_key = encode_meta_key(name);
        int res = GetZSetMetaVal(meta_key, zv);
        if (res != 1) {
            return -1;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<ZIterator>(this->zscan_internal(name, "", score_start, score_end, INT_MAX, Iterator::FORWARD, zv.version, snapshot));
    while(it->next()){
        count ++;
    }

    return count;
}

int64_t SSDBImpl::zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end) {
    int64_t count = 0;

    ZSetMetaVal zv;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock l(&mutex_record_, name.String());
        std::string meta_key = encode_meta_key(name);
        int res = GetZSetMetaVal(meta_key, zv);
        if (res != 1) {
            return -1;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    auto it = std::unique_ptr<ZIterator>(this->zscan_internal(name, "", score_start, score_end, INT_MAX, Iterator::FORWARD, zv.version, snapshot));

    while(it->next()){
        count ++;
        int ret = this->zdel(name, it->key);
        if(ret == -1){
            return -1;
        }
    }

    return count;
}

int SSDBImpl::zdel(const Bytes &name, const Bytes &key) {
    RecordLock l(&mutex_record_, name.String());
    leveldb::WriteBatch batch;

    int ret = zdel_one(this, batch, name, key);
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, batch, name, -ret) == -1) {
                return -1;
            }
        }
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if (!s.ok()) {
            log_error("zdel error: %s", s.ToString().c_str());
            return -1;
        }
    }
    return ret;
}

int SSDBImpl::zincr(const Bytes &name, const Bytes &key, double by, double *new_val) {
    RecordLock l(&mutex_record_, name.String());
    leveldb::WriteBatch batch;

    double old_score = 0;
    int ret = this->zget(name, key, &old_score);

    if (ret == -1) {
        return -1;
    } else if (ret == 0) {
        *new_val = by;
    } else {
        *new_val = old_score + by;
    }

    if (*new_val > ZSET_SCORE_MAX || *new_val < ZSET_SCORE_MIN) {
        return -1;
    }

    ret = zset_one(this, batch, name, key, *new_val);
    if (ret == -1) {
        return -1;
    }
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, batch, name, ret) == -1) {
                return -1;
            }
        }
        leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &(batch));
        if (!s.ok()) {
            log_error("zset error: %s", s.ToString().c_str());
            return -1;
        }
    }
    return 1;
}

int64_t SSDBImpl::zsize(const Bytes &name) {
    ZSetMetaVal zv;
    std::string size_key = encode_meta_key(name);
    int ret = GetZSetMetaVal(size_key, zv);
    if (ret <= 0) {
        return ret;
    } else {
        return (int64_t) zv.length;
    }
}

int SSDBImpl::GetZSetMetaVal(const std::string &meta_key, ZSetMetaVal &zv) {
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok() && !s.IsNotFound()) {
        return -1;
    } else {
        int ret = zv.DecodeMetaVal(meta_val);
        if (ret == -1) {
            return -1;
        } else if (zv.del == KEY_DELETE_MASK) {
            return 0;
        } else if (zv.type != DataType::ZSIZE){
            return -1;
        }
    }
    return 1;
}

//zscore
int SSDBImpl::zget(const Bytes &name, const Bytes &key, double *score) {
    *score = 0;

    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetZSetMetaVal(meta_key, zv);
    if (ret != 1) {
        return ret;
    }

    std::string str_score;
    std::string dbkey = encode_zset_key(name, key, zv.version);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), dbkey, &str_score);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("%s", s.ToString().c_str());
        return -1;
    }

    *score = *((double *) (str_score.data()));
    return 1;
}

ZIterator* SSDBImpl::zscan_internal(const Bytes &name, const Bytes &key_start,
                                    const Bytes &score_start, const Bytes &score_end,
                                    uint64_t limit, Iterator::Direction direction, uint16_t version,
                                    const leveldb::Snapshot *snapshot) {
    if (direction == Iterator::FORWARD) {
        std::string start, end;
        if (score_start.empty()) {
            start = encode_zscore_key(name, key_start, ZSET_SCORE_MIN, version);
        } else {
            start = encode_zscore_key(name, key_start, score_start.Double(), version);
        }
        if (score_end.empty()) {
            end = encode_zscore_key(name, "", ZSET_SCORE_MAX, version);
        } else {
            end = encode_zscore_key(name, "", score_end.Double(), version);
        }
        return new ZIterator(this->iterator(start, end, limit, snapshot), name, version);
    } else {
        std::string start, end;
        if (score_start.empty()) {
            start = encode_zscore_key(name, key_start, ZSET_SCORE_MAX, version);
        } else {
            if (key_start.empty()) {
                start = encode_zscore_key(name, "", score_start.Double(), version);
            } else {
                start = encode_zscore_key(name, key_start, score_start.Double(), version);
            }
        }
        if (score_end.empty()) {
            end = encode_zscore_key(name, "", ZSET_SCORE_MIN, version);
        } else {
            end = encode_zscore_key(name, "", score_end.Double(), version);
        }
        return new ZIterator(this->rev_iterator(start, end, limit, snapshot), name, version);
    }
}

int64_t SSDBImpl::zrank(const Bytes &name, const Bytes &key) {
    ZSetMetaVal zv;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock l(&mutex_record_, name.String());
        std::string meta_key = encode_meta_key(name);
        int res = GetZSetMetaVal(meta_key, zv);
        if (res != 1) {
            return -1;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    bool found = false;
    auto it = std::unique_ptr<ZIterator>(this->zscan_internal(name, "", "", "", INT_MAX, Iterator::FORWARD, zv.version, snapshot));
    uint64_t ret = 0;
    while (true) {
        if (!it->next()) {
            break;
        }
        if (key == it->key) {
            found = true;
            break;
        }
        ret++;
    }
    return found ? ret : -1;
}

int64_t SSDBImpl::zrrank(const Bytes &name, const Bytes &key) {
    ZSetMetaVal zv;
    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordLock l(&mutex_record_, name.String());

        std::string meta_key = encode_meta_key(name);
        int res = GetZSetMetaVal(meta_key, zv);
        if (res != 1) {
            return -1;
        }

        snapshot = ldb->GetSnapshot();
    }

    SnapshotPtr spl(ldb, snapshot); //auto release
    bool found = false;
    auto it = std::unique_ptr<ZIterator>(this->zscan_internal(name, "", "", "", INT_MAX, Iterator::BACKWARD, zv.version, snapshot));
    uint64_t ret = 0;
    while (true) {
        if (!it->next()) {
            break;
        }
        if (key == it->key) {
            found = true;
            break;
        }
        ret++;
    }
    return found ? ret : -1;
}

ZIterator *SSDBImpl::zrange(const Bytes &name, uint64_t offset, uint64_t limit, const leveldb::Snapshot** snapshot) {
    uint16_t version = 0;
    ZSetMetaVal zv;

    {
        RecordLock l(&mutex_record_, name.String());

        std::string meta_key = encode_meta_key(name);
        int ret = GetZSetMetaVal(meta_key, zv);
        if (0 == ret && zv.del == KEY_DELETE_MASK){
            version = zv.version+(uint16_t)1;
        } else if (ret > 0){
            version = zv.version;
        } else{
            version = 0;
        }
        *snapshot = ldb->GetSnapshot();
    }


    if (offset + limit > limit) {
        limit = offset + limit;
    }
    ZIterator *it = this->zscan_internal(name, "", "",  "", limit, Iterator::FORWARD, version, *snapshot);
    it->skip(offset);
    return it;
}

ZIterator *SSDBImpl::zrrange(const Bytes &name, uint64_t offset, uint64_t limit, const leveldb::Snapshot** snapshot) {
    uint16_t version = 0;
    ZSetMetaVal zv;

    {
        RecordLock l(&mutex_record_, name.String());
        std::string meta_key = encode_meta_key(name);
        int ret = GetZSetMetaVal(meta_key, zv);
        if (0 == ret && zv.del == KEY_DELETE_MASK){
            version = zv.version+(uint16_t)1;
        } else if (ret > 0){
            version = zv.version;
        } else{
            version = 0;
        }

        *snapshot = ldb->GetSnapshot();
    }

    if (offset + limit > limit) {
        limit = offset + limit;
    }
    ZIterator *it = this->zscan_internal(name, "", "", "", limit, Iterator::BACKWARD, version, *snapshot);
    it->skip(offset);
    return it;
}

ZIterator *SSDBImpl::zscan(const Bytes &name, const Bytes &key,
                           const Bytes &score_start, const Bytes &score_end, uint64_t limit) {
    uint16_t version = 0;
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int res = GetZSetMetaVal(meta_key, zv);
    if (res == 1) {
        version = zv.version;
    }

    std::string score;
    // if only key is specified, load its value
    if (!key.empty() && score_start.empty()) {
        double d_score = 0;
        this->zget(name, key, &d_score);
        score = str(d_score);

    } else {
        score = score_start.String();
    }
    return this->zscan_internal(name, key, score, score_end, limit, Iterator::FORWARD, version);
}


// returns the number of newly added items
static int zset_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, double score) {
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = ssdb->GetZSetMetaVal(meta_key, zv);
    if (ret == -1) {
        return -1;
    } else if (ret == 0 && zv.del == KEY_DELETE_MASK) {
        uint16_t cur_version;
        if (zv.version == UINT16_MAX){
            cur_version = 0;
        } else{
            cur_version = (uint16_t) (zv.version + 1);
        }
        zset_internal(ssdb, batch, name, key, score, cur_version);
        ret = 1;
    } else if (ret == 0) {
        zset_internal(ssdb, batch, name, key, score, 0);
        ret = 1;
    } else {
        double old_score = 0;

        int found = ssdb->zget(name, key, &old_score);
        if (found == -1) {
            return -1;
        }

        if (found == 0) {
            zset_internal(ssdb, batch, name, key, score, zv.version);

        } else if (found == 1) {
            if (fabs(old_score - score) < eps) {
                //same
            } else {
                string old_score_key = encode_zscore_key(name, key, old_score, zv.version);
                batch.Delete(old_score_key);
                zset_internal(ssdb, batch, name, key, score, zv.version);
            }
            return 0;
        } else {
            //error
            return -1;
        }
    }

    return ret;
}

void zset_internal(const SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, double new_score,
                   uint16_t cur_version) {
    string zkey = encode_zset_key(name, key, cur_version);

    string buf;
    buf.append((char *) (&new_score), sizeof(double));

    string score_key = encode_zscore_key(name, key, new_score, cur_version);

    batch.Put(zkey, buf);
    batch.Put(score_key, "");
}

static int zdel_one(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key) {
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = ssdb->GetZSetMetaVal(meta_key, zv);
    if (ret == -1) {
        return -1;
    } else if (ret == 0) {
        return 0;
    } else {
        double old_score = 0;
        int found = ssdb->zget(name, key, &old_score);
        if (found == -1) {
            return -1;
        } else if (found == 0) {
            return 0;
        } else {
            //found

            std::string old_score_key = encode_zscore_key(name, key, old_score, zv.version);
            std::string old_zset_key = encode_zset_key(name, key, zv.version);

            batch.Delete(old_score_key);
            batch.Delete(old_zset_key);
        }
    }

    return 1;
}

static int incr_zsize(SSDBImpl *ssdb, leveldb::WriteBatch &batch, const Bytes &name, int64_t incr) {
    ZSetMetaVal zv;
    std::string size_key = encode_meta_key(name);
    int ret = ssdb->GetZSetMetaVal(size_key, zv);
    if (ret == -1) {
        return ret;
    } else if (ret == 0 && zv.del == KEY_DELETE_MASK) {
        if (incr > 0) {
            uint16_t version;
            if (zv.version == UINT16_MAX){
                version = 0;
            } else{
                version = (uint16_t) (zv.version + 1);
            }
            std::string size_val = encode_zset_meta_val((uint64_t) incr, version);
            batch.Put(size_key, size_val);
        } else {
            return -1;
        }
    } else if (ret == 0) {
        if (incr > 0) {
            std::string size_val = encode_zset_meta_val((uint64_t) incr);
            batch.Put(size_key, size_val);
        } else {
            return -1;
        }
    } else {
        uint64_t len = zv.length;
        if (incr > 0) {
            len += incr;
        } else if (incr < 0) {
            uint64_t u64 = static_cast<uint64_t>(-incr);
            if (len < u64) {
                return -1;
            }
            len = len - u64;
        }
        if (len == 0) {
            std::string del_key = encode_delete_key(name.String(), zv.version);
            std::string meta_val = encode_zset_meta_val(zv.length, zv.version, KEY_DELETE_MASK);
            batch.Put(del_key, "");
            batch.Put(size_key, meta_val);
            ssdb->edel_one(batch, name); //del expire ET key
        } else {
            std::string size_val = encode_zset_meta_val(len, zv.version);
            batch.Put(size_key, size_val);
        }

    }
    return 0;
}

int64_t SSDBImpl::zfix(const Bytes &name) {
    RecordLock l(&mutex_record_, name.String());

    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetZSetMetaVal(meta_key, zv);
    if (ret != 1) {
        return ret;
    }

    std::string it_start, it_end;
    Iterator *it;
    leveldb::Status s;
    int64_t size = 0;
    int64_t old_size;

    it_start = encode_zscore_key(name, "", ZSET_SCORE_MIN, zv.version);
    it_end = encode_zscore_key(name, "", ZSET_SCORE_MAX, zv.version);
    it = this->iterator(it_start, it_end, UINT64_MAX);

    while (it->next()) {
        Bytes ks = it->key();
        //Bytes vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if (ks.data()[0] != DataType::ZSCORE) {
            break;
        }
//        std::string name2, key, score;
//        if (decode_zscore_key(ks, &name2, &key, &score) == -1) {
//            size = -1;
//            break;
//        }
        ZScoreItemKey zk;
        if(zk.DecodeItemKey(ks.String()) == -1){
            size = -1;
            break;
        }

        if (name != zk.key) {
            break;
        }
        size++;


        std::string buf = encode_zset_key(name, zk.field, zv.version);
        std::string score2;
        s = ldb->Get(leveldb::ReadOptions(), buf, &score2);
        if (!s.ok() && !s.IsNotFound()) {
            log_error("zget error: %s", s.ToString().c_str());
            size = -1;
            break;
        }

        double score = *((double *) (score2.data()));

        if (s.IsNotFound() || score != zk.score) {

            log_info("fix incorrect zset item, name: %s, key: %s, score: %s",
                     hexmem(name.data(), name.size()).c_str(),
                     hexmem(zk.field.data(), zk.field.size()).c_str(),
                     hexmem(score2.data(), score2.size()).c_str()
            );

            string score_buf;
            score_buf.append((char *) (&zk.score), sizeof(double));

            s = ldb->Put(leveldb::WriteOptions(), buf, score_buf);
            if (!s.ok()) {
                log_error("db error! %s", s.ToString().c_str());
                size = -1;
                break;
            }
        }
    }
    delete it;
    if (size == -1) {
        return -1;
    }

    old_size = this->zsize(name);
    if (old_size == -1) {
        return -1;
    }
    if (old_size != size) {
        log_info("fix zsize, name: %s, size: %"
                         PRId64
                         " => %"
                         PRId64,
                 hexmem(name.data(), name.size()).c_str(), old_size, size);

         zv.length = static_cast<uint64_t>(size);
        std::string size_val = encode_zset_meta_val(zv.length,zv.version);

        if (size == 0) {
//            s = ldb->Delete(leveldb::WriteOptions(), size_key);
        } else {
            s = ldb->Put(leveldb::WriteOptions(), meta_key, size_val);
        }
    }

    //////////////////////////////////////////

    it_start = encode_zset_key(name, "", zv.version); //bug here
    it_end = encode_zset_key(name, "", zv.version);
    it = this->iterator(it_start, "", UINT64_MAX);
    size = 0;
    while (it->next()) {
        Bytes ks = it->key();
        //Bytes vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if (ks.data()[0] != DataType::ZSET) {
            break;
        }

        ZSetItemKey zk;
        zk.DecodeItemKey(ks.String());

        if (zk.DecodeItemKey(ks.String()) == -1){
            size = -1;
            break;
        }

        if (name != zk.key) {
            break;
        }

        size++;

        Bytes score = it->val();
        double d_score = *((double *) score.String().data());
        std::string buf = encode_zscore_key(name, zk.field, d_score);
        std::string score2;
        s = ldb->Get(leveldb::ReadOptions(), buf, &score2);
        if (!s.ok() && !s.IsNotFound()) {
            log_error("zget error: %s", s.ToString().c_str());
            size = -1;
            break;
        }
        if (s.IsNotFound()) {
            log_info("fix incorrect zset score, name: %s, key: %s, score: %s",
                     hexmem(name.data(), name.size()).c_str(),
                     hexmem(zk.field.data(), zk.field.size()).c_str(),
                     hexmem(score.data(), score.size()).c_str()
            );
            s = ldb->Put(leveldb::WriteOptions(), buf, "");
            if (!s.ok()) {
                log_error("db error! %s", s.ToString().c_str());
                size = -1;
                break;
            }
        }
    }
    delete it;
    if (size == -1) {
        return -1;
    }

    old_size = this->zsize(name);
    if (old_size == -1) {
        return -1;
    }
    if (old_size != size) {
        log_info("fix zsize, name: %s, size: %"
                         PRId64
                         " => %"
                         PRId64,
                 hexmem(name.data(), name.size()).c_str(), old_size, size);
        zv.length = static_cast<uint64_t>(size);
        std::string size_val = encode_zset_meta_val(zv.length,zv.version);

        if (size == 0) {
//            s = ldb->Delete(leveldb::WriteOptions(), meta_key);
        } else {
            s = ldb->Put(leveldb::WriteOptions(), meta_key, size_val);
        }
    }

    //////////////////////////////////////////

    return size;
}
