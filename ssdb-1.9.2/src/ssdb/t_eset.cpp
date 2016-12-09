//
// Created by zts on 16-12-5.
//

#include "ssdb_impl.h"

static void eset_internal(const SSDBImpl *ssdb, const Bytes &key, int64_t ts, char log_type);

static int eset_one(SSDBImpl *ssdb, const Bytes &key, int64_t ts, char log_type);


int SSDBImpl::eset(const Bytes &key, int64_t ts, char log_type) {
    Transaction trans(binlogs);

    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok()){
        return -1;
    } else{
        if (meta_val.size() >= 4 ){
            if (meta_val[3] == KEY_DELETE_MASK){
                return 0;
            }
        } else{
            return -1;
        }
    }

    int ret = eset_one(this, key, ts, log_type);
    if (ret >= 0) {
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            log_error("eset error: %s", s.ToString().c_str());
            return -1;
        }
        ret = 1;
    }

    return ret;
}

int SSDBImpl::esetNoLock(const Bytes &key, int64_t ts, char log_type) {
    int ret = eset_one(this, key, ts, log_type);
    if (ret >= 0) {
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            log_error("eset error: %s", s.ToString().c_str());
            return -1;
        }
    }

    return ret;
}


int SSDBImpl::edel(const Bytes &key, char log_type) {
    Transaction trans(binlogs);

    int ret = edel_one(key, log_type);
    if (ret >= 0) {
        leveldb::Status s = binlogs->commit();
        if (!s.ok()) {
            log_error("edel error: %s", s.ToString().c_str());
            return -1;
        }
    }

    return ret;
}

int SSDBImpl::edel_one(const Bytes &key, char log_type) {

    int64_t old_ts = 0;

    int found = eget(key, &old_ts);
    if (found == -1) {
        return -1;
    } else if (found == 0) {
        return 0;
    } else {
        std::string old_score_key = encode_escore_key(key, static_cast<uint64_t>(old_ts));
        std::string old_eset_key = encode_eset_key(key);

        this->binlogs->Delete(old_score_key);
        this->binlogs->Delete(old_eset_key);
        this->binlogs->add_log(log_type, BinlogCommand::EDEL, old_eset_key);
    }

    return 1;
}

int SSDBImpl::eget(const Bytes &key, int64_t *ts) {
    *ts = 0;

    int ret = check_meta_key(key);
    if (ret <= 0){
        return -2;
    }

    std::string str_score;
    std::string dbkey = encode_eset_key(key);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), dbkey, &str_score);
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


void eset_internal(const SSDBImpl *ssdb, const Bytes &key, int64_t ts, char log_type) {
    string ekey = encode_eset_key(key);

    string buf;
    buf.append((char *) (&ts), sizeof(int64_t));

    string score_key = encode_escore_key(key, static_cast<uint64_t>(ts));

    ssdb->binlogs->Put(ekey, buf);
    ssdb->binlogs->Put(score_key, "");
    ssdb->binlogs->add_log(log_type, BinlogCommand::ESET, ekey);
}


int eset_one(SSDBImpl *ssdb, const Bytes &key, int64_t ts, char log_type) {

    int ret = 0;

    int64_t old_ts = 0;

    int found = ssdb->eget(key, &old_ts);
    if (found == -1) {
        return -1;
    }

    if (found == 0) {
        eset_internal(ssdb, key, ts, log_type);

    } else if (found == 1) {
        if (old_ts == ts) {
            //same
        } else {
            string old_score_key = encode_escore_key(key, static_cast<uint64_t>(old_ts));
            ssdb->binlogs->Delete(old_score_key);
            eset_internal(ssdb, key, ts, log_type);
        }
        return 0;
    } else {
        //error
        return -1;
    }


    return ret;

}

int SSDBImpl::check_meta_key(const Bytes &key) {
    std::string meta_key = encode_meta_key(key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()){
        return 0;
    } else if (!s.ok()){
        return -1;
    } else{
        if (meta_val.size() >= 4 ){
            if (meta_val[3] == KEY_DELETE_MASK){
                return 0;
            }
        } else{
            return -1;
        }
    }
    return 1;
}
