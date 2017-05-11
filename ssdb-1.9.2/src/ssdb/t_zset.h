//
// Created by zts on 17-3-30.
//

#ifndef SSDB_T_ZSET_H
#define SSDB_T_ZSET_H


#include "ssdb_impl.h"

template <typename T>
int SSDBImpl::zsetNoLock(Context &ctx, const Bytes &name, const std::map<T, T> &sortedSet, int flags, int64_t *num) {
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    /* XX and NX options at the same time are not compatible. */
    if (nx && xx) {
        return SYNTAX_ERR;
    }

    //INCR option supports a single increment-element pair
    if (incr && sortedSet.size() > 1) {
        return SYNTAX_ERR;
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
    bool needCheck = false;

    if (ret < 0) {
        return ret;
    } else if (ret == 0) {
        needCheck = false;
    } else {
        ret = 1;
        needCheck = true;
    }

    double newscore;

    for (auto const &it : sortedSet) {
        const Bytes &key = it.first;
        const Bytes &val = it.second;

//        log_info("%s:%s" , hexmem(key.data(),key.size()).c_str(), hexmem(val.data(),val.size()).c_str());

        double score = val.Double();

        if (score >= ZSET_SCORE_MAX || score <= ZSET_SCORE_MIN) {
            return INVALID_DBL;
        }
        int retflags = flags;

        int retval = zset_one(batch, needCheck, name, key, score, zv.version, &retflags, &newscore);
        if (retval < 0) {
            return retval;
        }

        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
    }

    int iret = incr_zsize(ctx, name, batch, zv, added);
    if (iret < 0) {
        log_error("incr_zsize error");
        return iret;
    } else if (iret == 0) {

    }

    leveldb::Status s = CommitBatch(ctx, &(batch));
    if (!s.ok()) {
        log_error("zset error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    if (ch) {
        *num = added + updated;
        return (ret + *num) > 0 ? 1 : 0;
    } else {
        *num = added;
        return (ret + *num) > 0 ? 1 : 0;
    }
}



#endif //SSDB_T_ZSET_H
