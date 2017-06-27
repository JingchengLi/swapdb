//
// Created by zts on 17-6-6.
//

#include <net/server.h>
#include "ssdb_impl.h"

#include "redis/dump_encode.h"
#include "redis/rdb_decoder.h"

#include "t_hash.h"
#include "t_set.h"
#include "t_zset.h"
#include "t_list.h"

extern "C" {
#include "redis/ziplist.h"
#include "redis/intset.h"
#include "redis/sha1.h"
};


static bool getNextString(unsigned char *zl, unsigned char **p, std::string &ret_res);

template<typename T>
int decodeMetaVal(T &mv, const std::string &val) {

    int ret = mv.DecodeMetaVal(val);
    if (ret < 0) {
        return ret;
    }

    return 1;
}


int SSDBImpl::dump(Context &ctx, const Bytes &key, std::string *res, int64_t *pttl, bool compress) {
    *res = "none";

    int ret = 0;
    std::string meta_val;
    char dtype;

    const leveldb::Snapshot* snapshot = nullptr;

    {
        RecordKeyLock l(&mutex_record_, key.String());

        std::string meta_key = encode_meta_key(key);
        leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
        if (s.IsNotFound()) {
            return 0;
        }
        if (!s.ok()) {
            log_error("get error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }

        //decodeMetaVal
        if(meta_val.size()<4) {
            //invalid
            log_error("invalid MetaVal: %s", s.ToString().c_str());
            return INVALID_METAVAL;
        }

        char del = meta_val[POS_DEL];
        if (del != KEY_ENABLED_MASK){
            //deleted
            return 0;
        }

        dtype = meta_val[0]; //get snapshot if need
        switch (dtype) {
            case DataType::HSIZE:
            case DataType::SSIZE:
            case DataType::ZSIZE:
            case DataType::LSIZE:{
                snapshot = GetSnapshot();
                break;
            }

            default:break;
        }

        if (pttl != nullptr) {
            int64_t ex = 0;
            int64_t remain = 0;
            ret = eget(ctx, key, &ex);
            if (ret < 0) {
                return ret;
            }

            if (ret == 1) {
                remain = ex - time_ms();
                if (remain < 0)  {
                    //expired
                    return 0;
                }
            }

            *pttl = remain;
        }

    }

    SnapshotPtr spl(ldb, snapshot); //auto release

    DumpEncoder rdbEncoder(compress);

    if (rdbEncoder.rdbSaveObjectType(dtype) < 0) return -1;
    if (rdbSaveObject(ctx, key, dtype, meta_val, rdbEncoder, snapshot) < 0) return -1;
    if (rdbEncoder.encodeFooter() == -1) return -1;

    *res = rdbEncoder.toString();

    return 1;
}


int SSDBImpl::rdbSaveObject(Context &ctx, const Bytes &key, char dtype, const std::string &meta_val,
                            RedisEncoder &encoder, const leveldb::Snapshot *snapshot) {

    int ret = 0;

    switch (dtype) {
        case DataType::KV: {
            KvMetaVal kv;
            ret = decodeMetaVal(kv, meta_val);
            if (ret != 1) {
                return ret;
            }

            if (encoder.rdbSaveRawString(kv.value) == -1) return -1;
            break;
        }
        case DataType::HSIZE: {
            HashMetaVal hv;
            ret = decodeMetaVal(hv, meta_val);
            if (ret != 1) {
                return ret;
            }
            if (encoder.rdbSaveLen(hv.length) == -1) return -1;

            auto it = std::unique_ptr<HIterator>(this->hscan_internal(ctx, key, hv.version, snapshot));


            uint64_t cnt = 0;
            while (it->next()) {
                if (encoder.saveRawString(it->key) == -1) return -1;
                if (encoder.saveRawString(it->val) == -1) return -1;
                cnt++;
            }

            if (cnt != hv.length) {
                log_error("hash metakey length dismatch !!!!! found %d, length %d, key %s", cnt, hv.length,
                          hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::SSIZE: {
            SetMetaVal sv;
            ret = decodeMetaVal(sv, meta_val);
            if (ret != 1) {
                return ret;
            }

            if (encoder.rdbSaveLen(sv.length) == -1) return -1;

            auto it = std::unique_ptr<SIterator>(this->sscan_internal(ctx, key, sv.version, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                if (encoder.saveRawString(it->key) == -1) return -1;
                cnt++;
            }

            if (cnt != sv.length) {
                log_error("set metakey length dismatch !!!!! found %d, length %d, key %s", cnt, sv.length,
                          hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::ZSIZE: {
            ZSetMetaVal zv;
            ret = decodeMetaVal(zv, meta_val);
            if (ret != 1) {
                return ret;

            }

            if (encoder.rdbSaveLen(zv.length) == -1) return -1;

            auto it = std::unique_ptr<ZIterator>(
                    this->zscan_internal(ctx, key, "", "", -1, Iterator::FORWARD, zv.version, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                if (encoder.saveRawString(it->key) == -1) return -1;
//                rdbEncoder.saveDoubleValue(it->score);
                if (encoder.rdbSaveBinaryDoubleValue(it->score) == -1) return -1;
                cnt++;
            }

            if (cnt != zv.length) {
                log_error("zset metakey length dismatch !!!!! found %d, length %d, key %s", cnt, zv.length,
                          hexstr(key).c_str());
                return MKEY_DECODEC_ERR;
            }

            break;
        }
        case DataType::LSIZE: {
            ListMetaVal lv;
            ret = decodeMetaVal(lv, meta_val);
            if (ret != 1) {
                return ret;
            }

            if (encoder.rdbSaveLen(lv.length) == -1) return -1;

            //readOptions using snapshot
            leveldb::ReadOptions readOptions(false, true);
            readOptions.snapshot = snapshot;

            uint64_t rangelen = lv.length;
            uint64_t begin_seq = getSeqByIndex(0, lv);
            uint64_t cur_seq = begin_seq;

            std::string item_key = encode_list_key(key, cur_seq, lv.version);

            while (rangelen--) {

                update_list_key(item_key, cur_seq); //do not use encode_list_key , just update the seq

                std::string item_val;
                ret = GetListItemValInternal(item_key, &item_val, readOptions);
                if (1 != ret) {
                    return -1;
                }
                if (encoder.rdbSaveRawString(item_val) == -1) return -1;

                if (UINT64_MAX == cur_seq) {
                    cur_seq = 0;
                } else {
                    cur_seq++;
                }
            }

            break;
        }
        default:
            return INVALID_METAVAL;
    }

    return ret;
}


int SSDBImpl::restore(Context &ctx, const Bytes &key, int64_t expire, const Bytes &data, bool replace, std::string *res) {
    *res = "none";

    if (expire < 0) {
        return INVALID_EX_TIME;
    }

    RecordKeyLock l(&mutex_record_, key.String());

    int ret = 0;
    std::string meta_val;
    std::string meta_key = encode_meta_key(key);
    leveldb::Status s;

    bool found = true;

    s = ldb->Get(commonRdOpt, meta_key, &meta_val);
    if (!s.ok() && !s.IsNotFound()) {
        return STORAGE_ERR;
    }

    if (s.ok()) {

        if(meta_val.size()<4) {
            return INVALID_METAVAL;
        }

        if(meta_val[POS_DEL] == KEY_ENABLED_MASK){
            if (!replace) {
                //exist key no
                return BUSY_KEY_EXISTS;
            }

            leveldb::WriteBatch batch;
            mark_key_deleted(ctx, key, batch, meta_key, meta_val);

            s = ldb->Write(leveldb::WriteOptions(), &(batch));
            if(!s.ok()){
                return STORAGE_ERR;
            }

            meta_val[POS_DEL] = KEY_DELETE_MASK;
        }

    }

    RdbDecoder rdbDecoder(data.data(), data.size());
    bool ok = rdbDecoder.verifyDumpPayload();

    if (!ok) {
        log_warn("checksum failed %s:%s", hexmem(key.data(), key.size()).c_str(),hexmem(data.data(), data.size()).c_str());
        return INVALID_DUMP_STR;
    }

    if (expire < 0){
        return INVALID_EX_TIME;
    }

    uint64_t len = 0;

    int rdbtype = rdbDecoder.rdbLoadObjectType();

//    log_info("rdb type : %d", rdbtype);

    switch (rdbtype) {
        case RDB_TYPE_STRING: {

            string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            ret = this->quickKv(ctx, key, r, meta_key, meta_val, expire);

            break;
        }

        case RDB_TYPE_LIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            auto next = [&] (std::string &current) {
                if (len-- > 0) {
                    int t_ret = 0;
                    current = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                    if (t_ret != 0) {
                        return -1;
                    }
                    return 1;
                }
                return 0;
            };

            ret = this->quickList<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }
        case RDB_TYPE_LIST_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            auto next = [&] (std::string &t_key) {
                if (getNextString(zl, &p, t_key)) {
                }
                return 0;
            };

            ret = this->quickList<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }
        case RDB_TYPE_LIST_QUICKLIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            int t_ret = 0;
            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
            if (t_ret != 0) {
                return -1;
            }

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            len --;
            auto next = [&](std::string &current) {
                if (len >= 0) {

                    if (getNextString(zl, &p, current)) {
                        return 1;
                    } else {
                        if (len > 0) {
                            zipListStr = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                            if (t_ret != 0) {
                                return -1;
                            }

                            zl = (unsigned char *) zipListStr.data();
                            p = ziplistIndex(zl, 0);

                            len--;
                            return 2;//retry next block
                        } else {
                            return 0;
                        }
                    }
                }
                return 0;
            };

            ret = this->quickList<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }

        case RDB_TYPE_SET: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            auto next = [&] (std::string &current) {
                if (len-- > 0) {
                    int t_ret = 0;
                    current = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                    if (t_ret != 0) {
                        return -1;
                    }
                    return 1;
                }
                return 0;
            };

            ret = this->quickSet<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }
        case RDB_TYPE_SET_INTSET:{

            std::string insetStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            const intset *set = (const intset *)insetStr.data();
            len = intsetLen(set);

            uint32_t j = 0;
            auto next = [&] (std::string &current) {
                if (j < len) {
                    int64_t t_value;
                    if (intsetGet((intset *) set, j++ , &t_value) == 1) {
                        current = str(t_value);
                    } else {
                        return -1;
                    }
                    return 1;
                }
                return 0;
            };

            ret = this->quickSet<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;

        }

        case RDB_TYPE_ZSET_2:
        case RDB_TYPE_ZSET: {
            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            auto next = [&] (std::string &t_key, double *score) {
                if (len-- > 0) {
                    int t_ret = 0;
                    t_key = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                    if (t_ret != 0) {
                        return -1;
                    }

                    if (rdbtype == RDB_TYPE_ZSET_2) {
                        if (rdbDecoder.rdbLoadBinaryDoubleValue(score) == -1) return -1;
                    } else {
                        if (rdbDecoder.rdbLoadDoubleValue(score) == -1) return -1;
                    }

                    return 1;
                }
                return 0;
            };

            ret = this->quickZset<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }
        case RDB_TYPE_ZSET_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string t_val;
            auto next = [&] (std::string &t_key, double *score) {
                if (getNextString(zl, &p, t_key)) {
                    if(getNextString(zl, &p, t_val)) {
                        *score = Bytes(t_val).Double();
                        if (errno == EINVAL){
                            return INVALID_DBL;
                        }

                        if (*score >= ZSET_SCORE_MAX || *score <= ZSET_SCORE_MIN) {
                            return VALUE_OUT_OF_RANGE;
                        }
                        return 1;
                    } else {
                        log_error("value not found ");
                        return -1;
                    }
                }
                return 0;
            };

            ret = this->quickZset<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }

        case RDB_TYPE_HASH: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            auto next = [&] (std::string &t_key, std::string &t_val) {
                if (len-- > 0) {
                    int t_ret = 0;
                    t_key = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                    if (t_ret != 0) {
                        return -1;
                    }

                    t_val = rdbDecoder.rdbGenericLoadStringObject(&t_ret);
                    if (t_ret != 0) {
                        return -1;
                    }

                    return 1;
                }
                return 0;
            };

            ret = this->quickHash<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }
        case RDB_TYPE_HASH_ZIPLIST: {

            std::map<std::string,std::string> tmp_map;

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            auto next = [&] (std::string &t_key, std::string &t_val) {
                if (getNextString(zl, &p, t_key)) {
                    if(getNextString(zl, &p, t_val)) {
                        return 1;
                    } else {
                        log_error("value not found ");
                        return -1;
                    }
                }
                return 0;
            };

            ret = this->quickHash<decltype(next)>(ctx, key, meta_key, meta_val, next);

            break;
        }

        case RDB_TYPE_HASH_ZIPMAP: {
            /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
            return -1;
            break;
        }
//        case RDB_TYPE_MODULE: break;
        default:
            log_error("Unknown RDB encoding type %d %s:%s", rdbtype, hexmem(key.data(), key.size()).c_str(),(data.data(), data.size()));
            return -1;
    }


    if (expire > 0 && (rdbtype != RDB_TYPE_STRING)) {
        leveldb::WriteBatch batch;
        expiration->expireAt(ctx, key, expire + time_ms(), batch, false);
        s = CommitBatch(ctx, &(batch));
        if (!s.ok()) {
            log_error("[restore] expireAt error: %s", s.ToString().c_str());
            return STORAGE_ERR;
        }

    }

    *res = "OK";
    return ret;
}



bool getNextString(unsigned char *zl, unsigned char **p, std::string &ret_res) {

    unsigned char *value;
    unsigned int sz;
    long long longval;

    if (ziplistGet(*p, &value, &sz, &longval)) {
        if (!value) {
            /* Write the longval as a string so we can re-add it */
            //TODO check str with ll2string in redis
            ret_res = str((int64_t) longval);
        } else {
            ret_res.assign((char *)value, sz);
        }

        *p = ziplistNext(zl, *p);
        return true;
    }

    return false;

}

int SSDBImpl::parse_replic(Context &ctx, const std::vector<Bytes> &kvs) {
    if (kvs.size()%2 != 0){
        return -1;
    }

    leveldb::WriteBatch batch;
    leveldb::WriteOptions writeOptions;
    writeOptions.disableWAL = true;

    auto it = kvs.begin();
    for(; it != kvs.end(); it += 2){
        const auto& key = *it;
        const auto& val = *(it+1);
        batch.Put(slice(key), slice(val));
    }

    leveldb::Status s = ldb->Write(writeOptions , &(batch));
    if(!s.ok()){
        log_error("write leveldb error: %s", s.ToString().c_str());
        return -1;
    }

    return 0;
}

int SSDBImpl::parse_replic(Context &ctx, const std::vector<std::string> &kvs) {
    if (kvs.size()%2 != 0){
        return -1;
    }

    leveldb::WriteBatch batch;
    leveldb::WriteOptions writeOptions;
    writeOptions.disableWAL = true;

    auto it = kvs.begin();
    for(; it != kvs.end(); it += 2){
        const auto& key = *it;
        const auto& val = *(it+1);
        batch.Put(slice(key), slice(val));
    }

    leveldb::Status s = ldb->Write(writeOptions , &(batch));
    if(!s.ok()){
        log_error("write leveldb error: %s", s.ToString().c_str());
        return -1;
    }

    return 0;
}


int SSDBImpl::scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) {
    // ignore cursor

    std::string start;
    if(cursor == "0") {
        start.append(1, DataType::META);
    } else {
        redisCursorService.FindElementByRedisCursor(cursor.String(), start);
    }


    Iterator* iter = iterator(start, "", -1);

    auto mit = std::unique_ptr<MIterator>(new MIterator(iter));

    bool end = doScanGeneric<MIterator>(mit.get(), pattern, limit, resp);

    if (!end) {
        //get new;
        uint64_t tCursor = redisCursorService.GetNewRedisCursor(iter->key().String()); //we already got it->next
        resp[1] = str(tCursor);
    }

    return 1;

}


#ifdef USE_LEVELDB
#else
#include "util/crc32c.h"
#endif

int SSDBImpl::digest(std::string *val) {

    auto snapshot = GetSnapshot();
    SnapshotPtr spl(ldb, snapshot); //auto release
    auto mit = std::unique_ptr<Iterator>((iterator("", "", -1)));

#ifdef USE_LEVELDB
    unsigned char digest[20];
    memset(digest,0,20); /* Start with a clean result */

    while(mit->next()){
        mixDigest(digest, (void *) (mit->key().data()), (size_t) mit->key().size());
        mixDigest(digest, (void *) (mit->val().data()), (size_t) mit->val().size());
    }

    std::string tmp;
    for (int i = 0; i < 20; ++i) {
        char vbuf[2] = {0};
        snprintf(vbuf, sizeof(vbuf), "%02x", digest[i]);
        tmp.append(vbuf);
    }

    *val = tmp;

#else
    uint32_t crc = 0;
    while (mit->next()) {
        crc = leveldb::crc32c::Extend(crc, mit->key().data(), (size_t) mit->key().size());
        crc = leveldb::crc32c::Extend(crc, mit->val().data(), (size_t) mit->val().size());
    }

#endif

    *val = str((uint64_t)crc);

    return 0;
}


/*
 * General API
 */
int SSDBImpl::redisCursorCleanup() {
    redisCursorService.ClearExpireRedisCursor();
    return 1;
}

int SSDBImpl::type(Context &ctx, const Bytes &key, std::string *type) {
    *type = "none";

    std::string meta_val;
    std::string meta_key = encode_meta_key(key);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);

    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    //decodeMetaVal
    if(meta_val.size()<4) {
        //invalid
        log_error("invalid MetaVal: %s", s.ToString().c_str());
        return INVALID_METAVAL;
    }

    char del = meta_val[POS_DEL];
    if (del != KEY_ENABLED_MASK){
        //deleted
        return 0;
    }
    char mtype = meta_val[POS_TYPE];

    if (mtype == DataType::KV) {
        *type = "string";
    } else if (mtype == DataType::HSIZE) {
        *type = "hash";
    } else if (mtype == DataType::SSIZE) {
        *type = "set";
    } else if (mtype == DataType::ZSIZE) {
        *type = "zset";
    } else if (mtype == DataType::LSIZE) {
        *type = "list";
    } else {
        return MKEY_DECODEC_ERR;
    }

    return 1;
}

int SSDBImpl::exists(Context &ctx, const Bytes &key) {
    std::string meta_val;
    std::string meta_key = encode_meta_key(key);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }
    if (meta_val[POS_DEL] == KEY_ENABLED_MASK) {
        return 1;
    } else if (meta_val[POS_DEL] == KEY_DELETE_MASK){
        return 0;
    } else {
        return MKEY_DECODEC_ERR;
    }
}

