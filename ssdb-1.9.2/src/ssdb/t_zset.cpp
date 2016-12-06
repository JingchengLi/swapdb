/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <limits.h>
#include "../include.h"
#include "t_zset.h"


static int zset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, double score, char log_type);

static int zdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type);

static int incr_zsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

static ZIterator *ziterator(
        SSDBImpl *ssdb,
        const Bytes &name, const Bytes &key_start,
        const Bytes &score_start, const Bytes &score_end,
        uint64_t limit, Iterator::Direction direction, uint16_t version);

void zset_internal(const SSDBImpl *ssdb, const Bytes &name, const Bytes &key, double new_score, char log_type,
                   uint16_t cur_version);


int64_t SSDBImpl::zclear(const Bytes &name) {
    Transaction trans(binlogs);

    int num = ZDelKeyNoLock(name);

    if (num > 0) {
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            return -1;
        }
    }

    return num;
}


int SSDBImpl::ZDelKeyNoLock(const Bytes &name, char log_type) {
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int ret = GetZSetMetaVal(meta_key, zv);
    if (ret != 1) {
        return ret;
    }

    if (zv.length > MAX_NUM_DELETE) {
        std::string del_key = encode_delete_key(name.String(), zv.version);
        std::string meta_val = encode_hash_meta_val(zv.length, zv.version, KEY_DELETE_MASK);
        binlogs->Put(del_key, "");
        binlogs->Put(meta_key, meta_val);
        return zv.length;
    }

    std::unique_ptr<ZIterator> it(ziterator(this, name, "", "", "", INT_MAX, Iterator::FORWARD, zv.version));
    int num = 0;
    while (it->next()) {
        ret = zdel_one(this, name, it->key, log_type);
        if (-1 == ret) {
            return -1;
        } else if (ret > 0) {
            num++;
        }
    }

    if (num > 0) {
        if (incr_zsize(this, name, -num) == -1) {
            return -1;
        }
    }

    return num;
}

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::zset(const Bytes &name, const Bytes &key, const Bytes &score, char log_type) {
    Transaction trans(binlogs);

    double new_score = score.Double();

    if (new_score > ZSET_SCORE_MAX || new_score < ZSET_SCORE_MIN) {
        return -1;
    }

    int ret = zset_one(this, name, key, new_score, log_type);
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, name, ret) == -1) {
                log_error("incr_zsize error");

                return -1;
            }
        }
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            log_error("zset error: %s", s.ToString().c_str());
            return -1;
        }
    }
    return ret;
}

int SSDBImpl::zsetNoLock(const Bytes &name, const Bytes &key, double score, char log_type) {
    //TODO check score
    int ret = zset_one(this, name, key, score, log_type);
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, name, ret) == -1) {
                return -1;
            }
        }
    }
    return ret;
}

int SSDBImpl::zdel(const Bytes &name, const Bytes &key, char log_type) {
    Transaction trans(binlogs);

    int ret = zdel_one(this, name, key, log_type);
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, name, -ret) == -1) {
                return -1;
            }
        }
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            log_error("zdel error: %s", s.ToString().c_str());
            return -1;
        }
    }
    return ret;
}

int SSDBImpl::zincr(const Bytes &name, const Bytes &key, double by, double *new_val, char log_type) {
    Transaction trans(binlogs);

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

    ret = zset_one(this, name, key, *new_val, log_type);
    if (ret == -1) {
        return -1;
    }
    if (ret >= 0) {
        if (ret > 0) {
            if (incr_zsize(this, name, ret) == -1) {
                return -1;
            }
        }
        leveldb::Status s = binlogs->commit();
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

static ZIterator *ziterator(
        SSDBImpl *ssdb,
        const Bytes &name, const Bytes &key_start,
        const Bytes &score_start, const Bytes &score_end,
        uint64_t limit, Iterator::Direction direction, uint16_t version) {
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
        return new ZIterator(ssdb->iterator(start, end, limit), name, version);
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
        return new ZIterator(ssdb->rev_iterator(start, end, limit), name, version);
    }
}

int64_t SSDBImpl::zrank(const Bytes &name, const Bytes &key) {
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int res = GetZSetMetaVal(meta_key, zv);
    if (res != 1) {
        return -1;
    }

    bool found = false;
    std::unique_ptr<ZIterator> it(ziterator(this, name, "", "", "", INT_MAX, Iterator::FORWARD, zv.version));
    uint64_t ret = 0;
    while (true) {
        if (it->next() == false) {
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
    std::string meta_key = encode_meta_key(name);
    int res = GetZSetMetaVal(meta_key, zv);
    if (res != 1) {
        return -1;
    }

    bool found = false;
    std::unique_ptr<ZIterator> it(ziterator(this, name, "", "", "", INT_MAX, Iterator::BACKWARD, zv.version));
    uint64_t ret = 0;
    while (true) {
        if (it->next() == false) {
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

ZIterator *SSDBImpl::zrange(const Bytes &name, uint64_t offset, uint64_t limit) {
    uint16_t version = 0;
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int res = GetZSetMetaVal(meta_key, zv);
    if (res == 1) {
        version = zv.version;
    }

    if (offset + limit > limit) {
        limit = offset + limit;
    }
    ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::FORWARD, version);
    it->skip(offset);
    return it;
}

ZIterator *SSDBImpl::zrrange(const Bytes &name, uint64_t offset, uint64_t limit) {
    uint16_t version = 0;
    ZSetMetaVal zv;
    std::string meta_key = encode_meta_key(name);
    int res = GetZSetMetaVal(meta_key, zv);
    if (res == 1) {
        version = zv.version;
    }

    if (offset + limit > limit) {
        limit = offset + limit;
    }
    ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::BACKWARD, version);
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
    return ziterator(this, name, key, score, score_end, limit, Iterator::FORWARD, version);
}

ZIterator *SSDBImpl::zrscan(const Bytes &name, const Bytes &key,
                            const Bytes &score_start, const Bytes &score_end, uint64_t limit) {
    uint16_t version = 0; //TODO op later
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
    return ziterator(this, name, key, score, score_end, limit, Iterator::BACKWARD, version);
}

static void get_znames(Iterator *it, std::vector<std::string> *list) {
    while (it->next()) {
        Bytes ks = it->key();
        //dump(ks.data(), ks.size());

        ZScoreItemKey zk;
        if (zk.DecodeItemKey(ks.String()) == -1) {
            continue;
        }
        list->push_back(str(zk.score));
    }
}

//TODO unsupported . due to M+slot+key  . slot in meta key
int SSDBImpl::zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
                    std::vector<std::string> *list) {
    std::string start;
    std::string end;

    start = encode_zsize_key(name_s);
    if (!name_e.empty()) {
        end = encode_zsize_key(name_e);
    }

    Iterator *it = this->iterator(start, end, limit);
    get_znames(it, list);
    delete it;
    return 0;
}

int SSDBImpl::zrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
                     std::vector<std::string> *list) {
    std::string start;
    std::string end;

    start = encode_zsize_key(name_s);
    if (name_s.empty()) {
        start.append(1, 255);
    }
    if (!name_e.empty()) {
        end = encode_zsize_key(name_e);
    }

    Iterator *it = this->rev_iterator(start, end, limit);
    get_znames(it, list);
    delete it;
    return 0;
}

// returns the number of newly added items
static int zset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, double score, char log_type) {
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
        zset_internal(ssdb, name, key, score, log_type, cur_version);
        ret = 1;
    } else if (ret == 0) {
        zset_internal(ssdb, name, key, score, log_type, 0);
        ret = 1;
    } else {
        double old_score = 0;

        int found = ssdb->zget(name, key, &old_score);
        if (found == -1) {
            return -1;
        }

        if (found == 0) {
            zset_internal(ssdb, name, key, score, log_type, zv.version);

        } else if (found == 1) {
            if (fabs(old_score - score) < eps) {
                //same
            } else {
                string old_score_key = encode_zscore_key(name, key, old_score, zv.version);
                ssdb->binlogs->Delete(old_score_key);
                zset_internal(ssdb, name, key, score, log_type, zv.version);
            }
            return 0;
        } else {
            //error
            return -1;
        }

    }


    return ret;
}

void zset_internal(const SSDBImpl *ssdb, const Bytes &name, const Bytes &key, double new_score, char log_type,
                   uint16_t cur_version) {
    string zkey = encode_zset_key(name, key, cur_version);

    string buf;
    buf.append((char *) (&new_score), sizeof(double));

    string score_key = encode_zscore_key(name, key, new_score, cur_version);

    ssdb->binlogs->Put(zkey, buf);
    ssdb->binlogs->Put(score_key, "");
    ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET, zkey);
}

static int zdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type) {
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

            ssdb->binlogs->Delete(old_score_key);
            ssdb->binlogs->Delete(old_zset_key);
            ssdb->binlogs->add_log(log_type, BinlogCommand::ZDEL, old_zset_key);

        }
    }

    return 1;
}

static int incr_zsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr) {
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
            ssdb->binlogs->Put(size_key, size_val);
        } else {
            return -1;
        }
    } else if (ret == 0) {
        if (incr > 0) {
            std::string size_val = encode_zset_meta_val((uint64_t) incr);
            ssdb->binlogs->Put(size_key, size_val);
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
            std::string meta_val = encode_hash_meta_val(zv.length, zv.version, KEY_DELETE_MASK);
            ssdb->binlogs->Put(del_key, "");
            ssdb->binlogs->Put(size_key, meta_val);
        } else {
            std::string size_val = encode_zset_meta_val(len, zv.version);
            ssdb->binlogs->Put(size_key, size_val);
        }

    }
    return 0;
}

int64_t SSDBImpl::zfix(const Bytes &name) {
    Transaction trans(binlogs);
    std::string it_start, it_end;
    Iterator *it;
    leveldb::Status s;
    int64_t size = 0;
    int64_t old_size;

    it_start = encode_zscore_key(name, "", ZSET_SCORE_MIN);
    it_end = encode_zscore_key(name, "", ZSET_SCORE_MAX);
    it = this->iterator(it_start, it_end, UINT64_MAX);
    size = 0;
    while (it->next()) {
        Bytes ks = it->key();
        //Bytes vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if (ks.data()[0] != DataType::ZSCORE) {
            break;
        }
        std::string name2, key, score;
        if (decode_zscore_key(ks, &name2, &key, &score) == -1) {
            size = -1;
            break;
        }
        if (name != name2) {
            break;
        }
        size++;

        std::string buf = encode_zset_key(name, key);
        std::string score2;
        s = ldb->Get(leveldb::ReadOptions(), buf, &score2);
        if (!s.ok() && !s.IsNotFound()) {
            log_error("zget error: %s", s.ToString().c_str());
            size = -1;
            break;
        }
        if (s.IsNotFound() || score != score2) {
            log_info("fix incorrect zset item, name: %s, key: %s, score: %s",
                     hexmem(name.data(), name.size()).c_str(),
                     hexmem(key.data(), key.size()).c_str(),
                     hexmem(score.data(), score.size()).c_str()
            );
            s = ldb->Put(leveldb::WriteOptions(), buf, score);
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
        std::string size_key = encode_zsize_key(name);
        if (size == 0) {
            s = ldb->Delete(leveldb::WriteOptions(), size_key);
        } else {
            s = ldb->Put(leveldb::WriteOptions(), size_key, leveldb::Slice((char *) &size, sizeof(int64_t)));
        }
    }

    //////////////////////////////////////////

    it_start = encode_zset_key(name, "");
    it_end = encode_zset_key(name.String(), "");
    it = this->iterator(it_start, it_end, UINT64_MAX);
    size = 0;
    while (it->next()) {
        Bytes ks = it->key();
        //Bytes vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if (ks.data()[0] != DataType::ZSET) {
            break;
        }
        std::string name2, key;
        if (decode_zset_key(ks, &name2, &key) == -1) {
            size = -1;
            break;
        }
        if (name != name2) {
            break;
        }
        size++;
        Bytes score = it->val();

        double d_score = *((double *) score.String().data());
        std::string buf = encode_zscore_key(name, key, d_score);
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
                     hexmem(key.data(), key.size()).c_str(),
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
        std::string size_key = encode_zsize_key(name);
        if (size == 0) {
            s = ldb->Delete(leveldb::WriteOptions(), size_key);
        } else {
            s = ldb->Put(leveldb::WriteOptions(), size_key, leveldb::Slice((char *) &size, sizeof(int64_t)));
        }
    }

    //////////////////////////////////////////

    return size;
}
