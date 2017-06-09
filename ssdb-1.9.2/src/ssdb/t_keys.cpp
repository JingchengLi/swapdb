//
// Created by zts on 17-6-6.
//

#include "ssdb_impl.h"

#include "redis/rdb_encoder.h"
#include "redis/rdb_decoder.h"

#include "t_hash.h"
#include "t_set.h"
#include "t_zset.h"

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


int SSDBImpl::dump(Context &ctx, const Bytes &key, std::string *res, int64_t *pttl) {
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

    RdbEncoder rdbEncoder;

    switch (dtype) {
        case DataType::KV: {
            KvMetaVal kv;
            ret = decodeMetaVal(kv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_STRING);
            rdbEncoder.rdbSaveRawString(kv.value);
            break;
        }
        case DataType::HSIZE: {
            HashMetaVal hv;
            ret = decodeMetaVal(hv, meta_val);
            if (ret != 1) {
                return ret;
            }

            rdbEncoder.rdbSaveType(RDB_TYPE_HASH);
            rdbEncoder.rdbSaveLen(hv.length);

            auto it = std::unique_ptr<HIterator>(this->hscan_internal(ctx, key, hv.version, snapshot));


            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveRawString(it->val);
                cnt ++;
            }

            if (cnt != hv.length) {
                log_error("hash metakey length dismatch !!!!! found %d, length %d, key %s", cnt, hv.length, hexstr(key).c_str());
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

            rdbEncoder.rdbSaveType(RDB_TYPE_SET);
            rdbEncoder.rdbSaveLen(sv.length);

            auto it = std::unique_ptr<SIterator>(this->sscan_internal(ctx, key, sv.version, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                cnt ++;
            }

            if (cnt != sv.length) {
                log_error("set metakey length dismatch !!!!! found %d, length %d, key %s", cnt, sv.length, hexstr(key).c_str());
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

            rdbEncoder.rdbSaveType(RDB_TYPE_ZSET);
            rdbEncoder.rdbSaveLen(zv.length);

            auto it = std::unique_ptr<ZIterator>(this->zscan_internal(ctx, key, "", "", -1, Iterator::FORWARD, zv.version, snapshot));

            uint64_t cnt = 0;
            while (it->next()) {
                rdbEncoder.saveRawString(it->key);
                rdbEncoder.saveDoubleValue(it->score);
                cnt ++;
            }

            if (cnt != zv.length) {
                log_error("zset metakey length dismatch !!!!! found %d, length %d, key %s", cnt, zv.length, hexstr(key).c_str());
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

            rdbEncoder.rdbSaveType(RDB_TYPE_LIST);
            rdbEncoder.rdbSaveLen(lv.length);

            //readOptions using snapshot
            leveldb::ReadOptions readOptions(false, true);
            readOptions.snapshot = snapshot;

            uint64_t rangelen = lv.length;
            uint64_t begin_seq = getSeqByIndex(0, lv);
            uint64_t cur_seq = begin_seq;

            while (rangelen--) {

                std::string item_key = encode_list_key(key, cur_seq, lv.version);
                std::string item_val;
                ret = GetListItemValInternal(item_key, &item_val, readOptions);
                if (1 != ret) {
                    return -1;
                }
                rdbEncoder.rdbSaveRawString(item_val);

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

    rdbEncoder.encodeFooter();
    *res = rdbEncoder.toString();

    return 1;
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

#ifdef USE_LEVELDB
    s = ldb->Get(commonRdOpt, meta_key, &meta_val);
#else
    if (ldb->KeyMayExist(commonRdOpt, meta_key, &meta_val, &found)) {
        if (!found) {
            s = ldb->Get(commonRdOpt, meta_key, &meta_val);
        }
    } else {
        s = s.NotFound();
    }
#endif

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

        }

    }

    RdbDecoder rdbDecoder(data.data(), data.size());
    bool ok = rdbDecoder.verifyDumpPayload();

    if (!ok) {
        log_warn("checksum failed %s:%s", hexmem(key.data(), key.size()).c_str(),hexmem(data.data(), data.size()).c_str());
        return INVALID_DUMP_STR;
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

            int added = 0;
            ret = this->setNoLock(ctx, key, r, ((expire > 0 ? OBJ_SET_PX : OBJ_SET_NO_FLAGS)), expire, &added);

            break;
        }

        case RDB_TYPE_LIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            t_res.reserve(len);

            while (len--) {
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }
                t_res.emplace_back(r);
            }

            uint64_t len_t;
            ret = this->rpushNoLock(ctx, key, t_res, 0, &len_t);

            break;
        }

        case RDB_TYPE_SET: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::set<std::string> tmp_set;

            while (len--) {
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                tmp_set.emplace(r);
            }

            int64_t num = 0;
            ret = this->saddNoLock<std::string>(ctx, key, tmp_set, &num);

            break;
        }

        case RDB_TYPE_ZSET_2:
        case RDB_TYPE_ZSET: {
            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::map<std::string,std::string> tmp_map;

            while (len--) {
                std::string r = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                double score;
                if (rdbtype == RDB_TYPE_ZSET_2) {
                    if (rdbDecoder.rdbLoadBinaryDoubleValue(&score) == -1) return -1;
                } else {
                    if (rdbDecoder.rdbLoadDoubleValue(&score) == -1) return -1;
                }

                tmp_map[r] = str(score);
            }

            int64_t num = 0;
            ret = zsetNoLock(ctx, key, tmp_map, ZADD_NONE, &num);

            break;
        }

        case RDB_TYPE_HASH: {

            std::map<std::string,std::string> tmp_map;

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;
            while (len--) {

                std::string field = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                std::string value = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                tmp_map[field] = value;
            }

            ret = this->hmsetNoLock<std::string>(ctx, key, tmp_map, false);

            break;
        }

        case RDB_TYPE_LIST_QUICKLIST: {

            if ((len = rdbDecoder.rdbLoadLen(NULL)) == RDB_LENERR) return -1;

            std::vector<std::string> t_res;
            t_res.reserve(len);

            while (len--) {

                std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
                if (ret != 0) {
                    return -1;
                }

                unsigned char *zl = (unsigned char *) zipListStr.data();
                unsigned char *p = ziplistIndex(zl, 0);

                std::string t_item;
                while (getNextString(zl, &p, t_item)) {
                    t_res.emplace_back(t_item);
                }
            }

            uint64_t len_t;
            ret = this->rpushNoLock(ctx, key, t_res, 0, &len_t);

            break;
        }

        case RDB_TYPE_HASH_ZIPMAP: {
            /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
            return -1;
            break;
        }
        case RDB_TYPE_SET_INTSET:{

            std::string insetStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            const intset *set = (const intset *)insetStr.data();
            len = intsetLen(set);

            std::set<std::string> tmp_set;

            for (uint32_t j = 0; j < len; ++j) {
                int64_t t_value;
                if (intsetGet((intset *) set, j , &t_value) == 1) {
                    tmp_set.emplace(str(t_value));
                } else {
                    return -1;
                }
            }

            int64_t num = 0;
            ret = this->saddNoLock<std::string>(ctx, key, tmp_set, &num);

            break;

        }
        case RDB_TYPE_LIST_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            std::vector<std::string> t_res;

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string r;
            while (getNextString(zl, &p, r)) {
                t_res.emplace_back(r);
            }

            uint64_t len_t;
            ret = this->rpushNoLock(ctx, key, t_res, 0, &len_t);

            break;
        }
        case RDB_TYPE_ZSET_ZIPLIST: {

            std::string zipListStr = rdbDecoder.rdbGenericLoadStringObject(&ret);
            if (ret != 0) {
                return -1;
            }

            std::map<std::string,std::string> tmp_map;

            unsigned char *zl = (unsigned char *) zipListStr.data();
            unsigned char *p = ziplistIndex(zl, 0);

            std::string t_item;
            while (getNextString(zl, &p, t_item)) {
                std::string value;
                if(getNextString(zl, &p, value)) {
                    tmp_map[t_item] = value;
                } else {
                    log_error("value not found ");
                    return -1;
                }
            }

            int64_t num = 0;
            ret = zsetNoLock(ctx, key, tmp_map, ZADD_NONE, &num);

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

            std::string field;
            while (getNextString(zl, &p, field)) {
                std::string value;

                if(getNextString(zl, &p, value)) {
                    tmp_map[field] = value;
                } else {
                    log_error("value not found ");
                    return -1;
                }
            }

            ret = this->hmsetNoLock(ctx, key, tmp_map, false);

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
    return 1;
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
            ret_res = std::string((char *)value, sz);
        }

        *p = ziplistNext(zl, *p);
        return true;
    }

    return false;

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
        const std::string& key = *it;
        const std::string& val = *(it+1);
        batch.Put(key, val);
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

int SSDBImpl::digest(std::string *val) {

    auto snapshot = GetSnapshot();
    SnapshotPtr spl(ldb, snapshot); //auto release

    unsigned char digest[20];
    memset(digest,0,20); /* Start with a clean result */

    auto mit = std::unique_ptr<Iterator>((iterator("", "", -1)));
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

