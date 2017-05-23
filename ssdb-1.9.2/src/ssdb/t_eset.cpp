//
// Created by zts on 16-12-5.
//

#include "ssdb_impl.h"


static int eset_one(Context &ctx, const Bytes &key, SSDBImpl *ssdb, leveldb::WriteBatch &batch, int64_t ts);


int SSDBImpl::eset(Context &ctx, const Bytes &key, int64_t ts) {
    RecordLock<Mutex> l(&mutex_record_, key.String());

    leveldb::WriteBatch batch;

    int ret = check_meta_key(ctx, key);
    if (ret <= 0) {
        return ret;
    }

    ret = eset_one(ctx, key, this, batch, ts);
    if (ret >= 0) {
        leveldb::Status s = CommitBatch(ctx, &(batch));
        if (!s.ok()) {
            log_error("eset error: %s", s.ToString().c_str());
            return -1;
        }
    }

    return ret;
}


int SSDBImpl::edel_one(Context &ctx, const Bytes &key,leveldb::WriteBatch &batch) {

    int64_t old_ts = 0;

    int found = eget(ctx, key, &old_ts);
    if (found == -1) {
        return -1;
    } else if (found == 0) {
        return 0;
    } else {
        std::string old_score_key = encode_escore_key(key, static_cast<uint64_t>(old_ts));
        std::string old_eset_key = encode_eset_key(key);

        batch.Delete(old_score_key);
        batch.Delete(old_eset_key);
    }

    return 1;
}

int SSDBImpl::eget(Context &ctx, const Bytes &key, int64_t *ts) {
    *ts = 0;

    std::string str_score;
    std::string dbkey = encode_eset_key(key);
    bool found = true;

    leveldb::Status s;


#ifdef USE_LEVELDB
    s = ldb->Get(commonRdOpt, dbkey, &str_score);
#else
    if (ldb->KeyMayExist(commonRdOpt, dbkey, &str_score, &found)) {
        if (!found) {
            s = ldb->Get(commonRdOpt, dbkey, &str_score);
        }
    } else {
        return 0;
    }
#endif

    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("%s", s.ToString().c_str());
        return -1;
    }

    *ts = *((uint64_t *) (str_score.data())); // int64_t -> uint64_t
    return 1;
}



int eset_one(Context &ctx, const Bytes &key, SSDBImpl *ssdb, leveldb::WriteBatch &batch, int64_t ts) {

    int ret = 1;

    int64_t old_ts = 0;

    int found = ssdb->eget(ctx, key, &old_ts);
    if (found == -1) {
        return -1;
    }

    if (found == 0) {
        //skip
    } else if (found == 1) {
        if (old_ts == ts) {
            //same
        } else {
            string old_score_key = encode_escore_key(key, static_cast<uint64_t>(old_ts));
            batch.Delete(old_score_key);
        }
    } else {
        //error
        return -1;
    }

    string ekey = encode_eset_key(key);

    string ekey_value((char *) (&ts), sizeof(int64_t));
    string score_key = encode_escore_key(key, static_cast<uint64_t>(ts));

    batch.Put(ekey, ekey_value);
    batch.Put(score_key, "");

    return ret;

}

int SSDBImpl::check_meta_key(Context &ctx, const Bytes &key) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        return 0;
    } else if (!s.ok()) {
        return -1;
    } else {
        if (meta_val.size() >= 4) {
            if (meta_val[3] == KEY_DELETE_MASK) {
                return 0;
            }
        } else {
            return -1;
        }
    }
    return 1;
}
