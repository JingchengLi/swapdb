/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_IMPL_H_
#define SSDB_IMPL_H_

#include <queue>
#include <atomic>

#ifdef USE_LEVELDB
#define SSDB_ENGINE "leveldb"

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

#include <memory>

#else

#define SSDB_ENGINE "rocksdb"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#define leveldb rocksdb
#endif
#include "util/log.h"
#include "util/config.h"
#include "util/PTimer.h"
#include "util/thread.h"

#include "ssdb.h"
#include "iterator.h"

#include "codec/decode.h"
#include "codec/encode.h"

#include "ssdb/ttl.h"
#include "t_cursor.h"

#define MAX_NUM_DELETE 10

inline
static leveldb::Slice slice(const Bytes &b){
	return leveldb::Slice(b.data(), b.size());
}


enum LIST_POSTION{
	HEAD,
	TAIL,
};


class SSDBImpl : public SSDB
{
private:
	friend class SSDB;
	leveldb::DB* ldb;
	leveldb::Options options;
	
	SSDBImpl();
public:
    ExpirationHandler *expiration;

	RedisCursorService redisCursorService;
	
	virtual ~SSDBImpl();

	virtual int flushdb();

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit,
							   const leveldb::Snapshot *snapshot=nullptr);
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit,
								   const leveldb::Snapshot *snapshot=nullptr);
	virtual const leveldb::Snapshot* GetSnapshot();
	virtual void ReleaseSnapshot(const leveldb::Snapshot* snapshot=nullptr);

	//void flushdb();
	virtual uint64_t size();
	virtual std::vector<std::string> info();
	virtual void compact();

	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Bytes &key, const Bytes &val);
	virtual int raw_del(const Bytes &key);
	virtual int raw_get(const Bytes &key, std::string *val);

	/* 	General	*/
	virtual int type(const Bytes &key, std::string *type);
	virtual int dump(const Bytes &key, std::string *res);
    virtual int restore(const Bytes &key, int64_t expire, const Bytes &data, bool replace, std::string *res);
	virtual int exists(const Bytes &key);
	virtual int parse_replic(const std::vector<std::string> &kvs);

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val, int flags);
	virtual int del(const Bytes &key);
	virtual int append(const Bytes &key, const Bytes &value, uint64_t *llen);

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val);
	virtual int incrbyfloat(const Bytes &key, long double by, long double *new_val);
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0);
	virtual int multi_del(const std::vector<Bytes> &keys, int offset=0);
	virtual int setbit(const Bytes &key, int64_t bitoffset, int on);
	virtual int getbit(const Bytes &key, int64_t bitoffset);
	
	virtual int get(const Bytes &key, std::string *val);
	virtual int getset(const Bytes &key, std::string *val, const Bytes &newval);
	virtual int getrange(const Bytes &key, int64_t start, int64_t end, std::string *res);
	// return (start, end]
	virtual int setrange(const Bytes &key, int64_t start, const Bytes &value, uint64_t *new_len);

	virtual int scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);
	/* hash */

	virtual int hmset(const Bytes &name, const std::map<Bytes,Bytes> &kvs);
	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val);
	virtual int hsetnx(const Bytes &name, const Bytes &key, const Bytes &val);
	virtual int hdel(const Bytes &name, const std::set<Bytes>& fields);

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val);
	virtual int hincrbyfloat(const Bytes &name, const Bytes &key, long double by, long double *new_val);
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);

	virtual int64_t hsize(const Bytes &name);
	virtual int hmget(const Bytes &name, const std::vector<std::string> &reqKeys, std::map<std::string, std::string> *val);
	virtual int hgetall(const Bytes &name, std::map<std::string, std::string> &val);
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val);
//	virtual HIterator* hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual int hscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);


    /*  list  */
    virtual int LIndex(const Bytes &key, const int64_t index, std::string *val);
    virtual int LLen(const Bytes &key, uint64_t *llen);
    virtual int LPop(const Bytes &key, std::string *val);
    virtual int LPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
    virtual int LPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int RPop(const Bytes &key, std::string *val);
	virtual int RPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int RPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int LSet(const Bytes &key, const int64_t index, const Bytes &val);
	virtual int lrange(const Bytes &key, int64_t start, int64_t end, std::vector<std::string> *list);
	virtual int ltrim(const Bytes &key, int64_t start, int64_t end);



    /* set */
    virtual int sadd(const Bytes &key, const std::set<Bytes> &members, int64_t *num);
    virtual int srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num);
	virtual int scard(const Bytes &key, uint64_t *llen);
    virtual int sismember(const Bytes &key, const Bytes &member);
    virtual int smembers(const Bytes &key, std::vector<std::string> &members);
	virtual int spop(const Bytes &key, std::vector<std::string> &members, int64_t popcnt);
	virtual int srandmember(const Bytes &key, std::vector<std::string> &members, int64_t cnt);
    virtual int sunion(const std::vector<Bytes> &keys, std::set<std::string>& members);
    virtual int sunionstore(const Bytes &destination, const std::vector<Bytes> &keys, int64_t *num);
	virtual int sscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);

	/* zset */
	virtual int multi_zset(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags);
	virtual int multi_zdel(const Bytes &name, const std::set<Bytes> &keys);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &name, const Bytes &key, double by, int flags, double *new_val);
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);

	int64_t zcount(const Bytes &name, const Bytes &score_start, const Bytes &score_end);
	int64_t zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end, int remove);

	virtual int64_t zsize(const Bytes &name);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, double *score);
	virtual int64_t zrank(const Bytes &name, const Bytes &key);
	virtual int64_t zrrank(const Bytes &name, const Bytes &key);
	virtual int zrange(const Bytes &name, const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score);
	virtual int zrrange(const Bytes &name, const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score);
    virtual int zrangebyscore(const Bytes &name, const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                int withscores, long offset, long limit);
    virtual int zrevrangebyscore(const Bytes &name, const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                int withscores, long offset, long limit);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual int zscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);

	virtual int64_t zfix(const Bytes &name);
	virtual int64_t zlexcount(const Bytes &name, const Bytes &key_start, const Bytes &key_end);



	/* eset */
	virtual int eset(const Bytes &key, int64_t ts);
	virtual int esetNoLock(const Bytes &key, int64_t ts);
	virtual int edel(const Bytes &key);
	virtual int eget(const Bytes &key, int64_t *ts);
	virtual int edel_one(leveldb::WriteBatch &batch, const Bytes &key);
    virtual int check_meta_key(const Bytes &key);


	//TODO
	int GetZSetItemVal(const std::string &item_key, double *score);

private:

	int SetGeneric(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &val, int flags, const int64_t expire);
    int GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv);

    int del_key_internal(leveldb::WriteBatch &batch, const Bytes &key);
    int mark_key_deleted(leveldb::WriteBatch &batch, const Bytes &key, const std::string &meta_key, std::string &meta_val);


    int GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv);
    int GetHashItemValInternal(const std::string &item_key, std::string *val);
    HIterator* hscan_internal(const Bytes &name, const Bytes &start, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);
    int incr_hsize(leveldb::WriteBatch &batch, const std::string &size_key, HashMetaVal &hv, const Bytes &name, int64_t incr);
    int hset_one(leveldb::WriteBatch &batch, const HashMetaVal &hv, bool check_exists, const Bytes &name, const Bytes &key, const Bytes &val);

    int GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv);
    int GetSetItemValInternal(const std::string &item_key);
    SIterator* sscan_internal(const Bytes &name, const Bytes &start, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);
    int incr_ssize(leveldb::WriteBatch &batch, const SetMetaVal &sv, const std::string &meta_key,const Bytes &key, int64_t incr);
    int sunion_internal(const std::vector<Bytes> &keys, int offset, std::set<std::string>& members);

    int GetListItemValInternal(const std::string &item_key, std::string *val, const leveldb::ReadOptions &options = leveldb::ReadOptions());
    int GetListMetaVal(const std::string &meta_key, ListMetaVal &lv);
    int doListPop(leveldb::WriteBatch &batch, ListMetaVal &meta_val, const Bytes &key, std::string &meta_key, LIST_POSTION lp, std::string *val);
    int DoLPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key, ListMetaVal &meta_val);
    int DoRPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key, ListMetaVal &meta_val);


    int GetZSetMetaVal(const std::string &meta_key, ZSetMetaVal &zv);
    ZIterator* zscan_internal(const Bytes &name, const Bytes &key_start,
										const Bytes &score_start, const Bytes &score_end,
										uint64_t limit, Iterator::Direction direction, uint16_t version,
										const leveldb::Snapshot *snapshot=nullptr);
    ZIteratorByLex* zscanbylex_internal(const Bytes &name, const Bytes &key_start, const Bytes &key_end,
							  uint64_t limit, Iterator::Direction direction, uint16_t version,
							  const leveldb::Snapshot *snapshot=nullptr);

	int setNoLock(const Bytes &key, const Bytes &val, int flags);
    int saddNoLock(const Bytes &key, const std::set<Bytes> &mem_set, int64_t *num);
    int hmsetNoLock(const Bytes &name, const std::map<Bytes,Bytes> &kvs, bool check_exists);
    int rpushNoLock(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	int zsetNoLock(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags);
	int zdelNoLock(const Bytes &name, const std::set<Bytes> &keys);
    int zrangeGeneric(const Bytes &name, const Bytes &begin, const Bytes &limit, std::vector<string> &key_score, int reverse);
    int genericZrangebyscore(const Bytes &name, const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                             int withscores, long offset, long limit, int reverse);

private:
	//    pthread_mutex_t mutex_bgtask_;
	Mutex mutex_bgtask_;
	std::atomic<bool> bgtask_flag_;
	pthread_t bg_tid_;
    std::queue<std::string> tasks_;
	CondVar bg_cv_;
    RecordMutex mutex_record_;

	void start();
	void stop();
    void load_delete_keys_from_db(int num);
    void delete_key_loop(const std::string& del_key);
    int  delete_meta_key(const DeleteKey& dk, leveldb::WriteBatch& batch);
	void runBGTask();
	static void* thread_func(void *arg);
};

uint64_t getSeqByIndex(int64_t index, const ListMetaVal &meta_val);


class SnapshotPtr{
private:

public:
	SnapshotPtr(leveldb::DB *ldb, const leveldb::Snapshot *snapshot) : ldb(ldb), snapshot(snapshot) {}

	virtual ~SnapshotPtr() {
		if (snapshot!=nullptr) {
			ldb->ReleaseSnapshot(snapshot);
		}
	}

	leveldb::DB* ldb;
	const leveldb::Snapshot* snapshot;

};


#endif
