/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_IMPL_H_
#define SSDB_IMPL_H_

#ifdef USE_LEVELDB
#define SSDB_ENGINE "leveldb"

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include <memory>

#else
#define SSDB_ENGINE "rocksdb"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#define leveldb rocksdb
#endif
#include "../util/log.h"
#include "../util/config.h"
#include "../util/PTimer.h"

#include "ssdb.h"
#include "binlog.h"
#include "iterator.h"
//#include "t_kv.h"
//#include "t_hash.h"
//#include "t_zset.h"
//#include "t_queue.h"
#include "ssdb/ttl.h"
#include "codec/decode.h"
#include "codec/encode.h"

#include <queue>
#include <atomic>
#include "util/thread.h"

#define MAX_NUM_DELETE 10

inline
static leveldb::Slice slice(const Bytes &b){
	return leveldb::Slice(b.data(), b.size());
}

class SSDBImpl : public SSDB
{
private:
	friend class SSDB;
	leveldb::DB* ldb;
	leveldb::Options options;
	
	SSDBImpl();
public:
	BinlogQueue *binlogs;
    ExpirationHandler *expiration;
	
	virtual ~SSDBImpl();

	virtual int flushdb();

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit,
							   const leveldb::Snapshot *snapshot=nullptr);
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit,
								   const leveldb::Snapshot *snapshot=nullptr);
	virtual const leveldb::Snapshot* GetSnapshot();
	virtual void ReleaseSnapshot(const leveldb::Snapshot* snapshot=nullptr);
	virtual const leveldb::Snapshot* GetSnapshot();

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
	virtual int parse_replic(const char* data, int& size);

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val);
	virtual int setnx(const Bytes &key, const Bytes &val);
	virtual int del(const Bytes &key);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val);
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0);
	virtual int multi_del(const std::vector<Bytes> &keys, int offset=0);
	virtual int setbit(const Bytes &key, int64_t bitoffset, int on);
	virtual int getbit(const Bytes &key, int64_t bitoffset);
	
	virtual int get(const Bytes &key, std::string *val);
	virtual int getset(const Bytes &key, std::string *val, const Bytes &newval);
	virtual int getrange(const Bytes &key, int64_t start, int64_t end, std::string *res);
	// return (start, end]

	/* hash */

	virtual int hmset(const Bytes &name, const std::map<Bytes,Bytes> &kvs);
	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val);
	virtual int hdel(const Bytes &name, const Bytes &key);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val);
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);

	virtual int64_t hsize(const Bytes &name);
	virtual int hmget(const Bytes &name, const std::vector<std::string> &reqKeys, std::map<std::string, std::string> *val);
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val);
	virtual HIterator* hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual HIterator* hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit, const leveldb::Snapshot** snapshot);
    int     GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv);
    int     GetHashItemValInternal(const std::string &item_key, std::string *val);

    /*  list  */
    virtual int LIndex(const Bytes &key, const int64_t index, std::string *val);
    virtual int LLen(const Bytes &key, uint64_t *llen);
    virtual int LPop(const Bytes &key, std::string *val);
    virtual int LPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int RPop(const Bytes &key, std::string *val);
	virtual int RPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int LSet(const Bytes &key, const int64_t index, const Bytes &val);
	virtual int lrange(const Bytes &key, int64_t start, int64_t end, std::vector<std::string> *list);
    int     GetListMetaVal(const std::string& meta_key, ListMetaVal& lv);
    int     GetListItemVal(const std::string& item_key, std::string* val, const leveldb::ReadOptions& options=leveldb::ReadOptions());
    int     DoLPush(leveldb::WriteBatch &batch, ListMetaVal &meta_val, const Bytes &key, const Bytes &val, std::string &meta_key);
    int     DoLPush(leveldb::WriteBatch &batch, ListMetaVal &meta_val, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key);
    int     DoFirstLPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, const std::string &meta_key, uint16_t version);
    void    PushFirstListItem(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &val, const std::string &meta_key, uint16_t version);
	int 	DoRPop(leveldb::WriteBatch &batch, ListMetaVal &meta_val, const Bytes &key, std::string &meta_key, std::string *val);
	int 	DoRPush(leveldb::WriteBatch &batch, ListMetaVal &meta_val, const Bytes &key, const Bytes &val, std::string &meta_key);

    int     DoRPush(leveldb::WriteBatch &batch, const Bytes &key, const std::vector<Bytes> &val, int offset, std::string &meta_key, ListMetaVal &meta_val);

    /* set */
    virtual int sadd(const Bytes &key, const Bytes &member);
    virtual int multi_sadd(const Bytes &key, const std::set<Bytes> &members, int64_t *num);
    virtual int multi_srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num);
	virtual int srem(const Bytes &key, const Bytes &member);
	virtual int scard(const Bytes &key, uint64_t *llen);
    virtual int sismember(const Bytes &key, const Bytes &member);
    virtual int smembers(const Bytes &key, std::vector<std::string> &members);
    virtual int sunion(const std::vector<Bytes> &keys, std::set<std::string>& members);
    virtual int sunionstore(const Bytes &destination, const std::vector<Bytes> &keys, int64_t *num);
	virtual SIterator* sscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit);

	/* zset */
	virtual int zset(const Bytes &name, const Bytes &key, const Bytes &score);
	virtual int multi_zset(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags);
	virtual int zdel(const Bytes &name, const Bytes &key);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &name, const Bytes &key, double by, double *new_val);
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);

	int64_t zcount(const Bytes &name, const Bytes &score_start, const Bytes &score_end);
	int64_t zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end);

	virtual int64_t zsize(const Bytes &name);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, double *score);
	virtual int64_t zrank(const Bytes &name, const Bytes &key);
	virtual int64_t zrrank(const Bytes &name, const Bytes &key);
	virtual ZIterator* zrange(const Bytes &name, int64_t offset, int64_t limit, const leveldb::Snapshot** snapshot);
	virtual ZIterator* zrrange(const Bytes &name, uint64_t offset, uint64_t limit, const leveldb::Snapshot** snapshot);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual ZIterator* zscan(const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit);

	virtual int64_t zfix(const Bytes &name);

	virtual int GetZSetMetaVal(const std::string &meta_key, ZSetMetaVal &zv);

    virtual int64_t qsize(const Bytes &name);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qfront(const Bytes &name, std::string *item);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qback(const Bytes &name, std::string *item);
	// @return -1: error, other: the new length of the queue
	virtual int64_t qpush_front(const Bytes &name, const Bytes &item);
	virtual int64_t qpush_back(const Bytes &name, const Bytes &item);
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int qpop_front(const Bytes &name, std::string *item);
	virtual int qpop_back(const Bytes &name, std::string *item);
	virtual int qfix(const Bytes &name);

	virtual int qslice(const Bytes &name, int64_t offset, int64_t limit,
			std::vector<std::string> *list);
	virtual int qget(const Bytes &name, int64_t index, std::string *item);
	virtual int qset(const Bytes &name, int64_t index, const Bytes &item);
	virtual int qset_by_seq(const Bytes &name, uint64_t seq, const Bytes &item);


	/* eset */
	virtual int eset(const Bytes &key, int64_t ts);
	virtual int esetNoLock(const Bytes &key, int64_t ts);
	virtual int edel(const Bytes &key);
	virtual int eget(const Bytes &key, int64_t *ts);
	virtual int edel_one(leveldb::WriteBatch &batch, const Bytes &key);
    virtual int check_meta_key(const Bytes &key);


private:
	int64_t _qpush(const Bytes &name, const Bytes &item, uint64_t front_or_back_seq);
	int _qpop(const Bytes &name, std::string *item, uint64_t front_or_back_seq);

	int SetGeneric(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &val, int flags, const int64_t expire);
    int KDel(const Bytes &key);
	int KDelNoLock(leveldb::WriteBatch &batch, const Bytes &key);
    int GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv);


    int del_key_internal(leveldb::WriteBatch &batch, const Bytes &key);
    int mark_key_deleted(leveldb::WriteBatch &batch, const Bytes &key, const std::string &meta_key, std::string &meta_val);

    HIterator* hscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);

    int GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv);
    int GetSetItemValInternal(const std::string &item_key);
    int sadd_one(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &member);
	int incr_ssize(leveldb::WriteBatch &batch, const SetMetaVal &sv, const std::string &meta_key,int ret , const Bytes &key, int64_t incr);
    int incr_ssize(leveldb::WriteBatch &batch, const Bytes &key, int64_t incr);
	int srem_one(leveldb::WriteBatch &batch, const Bytes &key, const Bytes &member);
    SIterator* sscan_internal(const Bytes &name, const Bytes &start, const Bytes &end, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);
    int sunion_internal(const std::vector<Bytes> &keys, int offset, std::set<std::string>& members);
	int saddNoLock(const Bytes &key, const std::set<Bytes> &mem_set, int64_t *num);

	int lGetCurrentMetaVal(const std::string &meta_key, ListMetaVal &meta_val, uint64_t *llen);

	ZIterator* zscan_internal(const Bytes &name, const Bytes &key_start,
										const Bytes &score_start, const Bytes &score_end,
										uint64_t limit, Iterator::Direction direction, uint16_t version,
										const leveldb::Snapshot *snapshot=nullptr);

	int setNoLock(const Bytes &key, const Bytes &val, int flags);
	int hmsetNoLock(const Bytes &name, const std::map<Bytes,Bytes> &kvs);
	int rpushNoLock(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen);
	int zsetNoLock(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags);

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
