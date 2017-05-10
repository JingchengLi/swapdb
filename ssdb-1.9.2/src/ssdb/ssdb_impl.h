/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_IMPL_H_
#define SSDB_IMPL_H_

#include <queue>
#include <atomic>
#include "include.h"
#include "common/context.hpp"

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


enum LIST_POSITION{
	HEAD,
	TAIL,
};

enum DIRECTION{
	FORWARD,
	BACKWARD,
};


class SSDBImpl : public SSDB
{
private:
	friend class SSDB;
	leveldb::DB* ldb;
	leveldb::Options options;

	RedisCursorService redisCursorService;

	uint64_t commitedIndex;
	uint64_t commitedTimestamp;

	uint64_t recievedIndex;
	uint64_t recievedTimestamp;

	SSDBImpl();
public:

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

	//void flushdb();
	virtual uint64_t size();
	virtual std::vector<std::string> info();
	virtual void compact();
	virtual int digest(std::string *val);
	virtual leveldb::Status CommitBatch(const Context &ctx, leveldb::WriteBatch* updates);
	virtual leveldb::Status CommitBatch(const Context &ctx, const leveldb::WriteOptions& options, leveldb::WriteBatch* updates);

	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Context &ctx, const Bytes &key,const Bytes &val);
	virtual int raw_del(const Context &ctx, const Bytes &key);
	virtual int raw_get(const Context &ctx, const Bytes &key,std::string *val);

	/* 	General	*/
	virtual int type(const Context &ctx, const Bytes &key,std::string *type);
	virtual int dump(const Context &ctx, const Bytes &key,std::string *res);
    virtual int restore(const Context &ctx, const Bytes &key,int64_t expire, const Bytes &data, bool replace, std::string *res);
	virtual int exists(const Context &ctx, const Bytes &key);
	virtual int parse_replic(const Context &ctx, const std::vector<std::string> &kvs);

	/* key value */

	virtual int set(const Context &ctx, const Bytes &key,const Bytes &val, int flags, int *added);
	virtual int del(const Context &ctx, const Bytes &key);
	virtual int append(const Context &ctx, const Bytes &key,const Bytes &value, uint64_t *llen);

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Context &ctx, const Bytes &key,int64_t by, int64_t *new_val);
	virtual int incrbyfloat(const Context &ctx, const Bytes &key,long double by, long double *new_val);
	virtual int multi_set(const Context &ctx, const std::vector<Bytes> &kvs, int offset=0);
	virtual int multi_del(const Context &ctx, const std::set<Bytes> &keys, int64_t *count);
	virtual int setbit(const Context &ctx, const Bytes &key,int64_t bitoffset, int on, int *res);
	virtual int getbit(const Context &ctx, const Bytes &key,int64_t bitoffset, int *res);
	
	virtual int get(const Context &ctx, const Bytes &key,std::string *val);
	virtual int getset(const Context &ctx, const Bytes &key,std::pair<std::string, bool> &val, const Bytes &newval);
	virtual int getrange(const Context &ctx, const Bytes &key,int64_t start, int64_t end, std::pair<std::string, bool> &res);
	// return (start, end]
	virtual int setrange(const Context &ctx, const Bytes &key,int64_t start, const Bytes &value, uint64_t *new_len);

	virtual int scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);
	/* hash */

	virtual int hmset(const Context &ctx, const Bytes &name,const std::map<Bytes,Bytes> &kvs);
	virtual int hset(const Context &ctx, const Bytes &name,const Bytes &key, const Bytes &val, int *added);
	virtual int hsetnx(const Context &ctx, const Bytes &name,const Bytes &key, const Bytes &val, int *added);
	virtual int hdel(const Context &ctx, const Bytes &name,const std::set<Bytes>& fields, int *deleted);

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Context &ctx, const Bytes &name,const Bytes &key, int64_t by, int64_t *new_val);
	virtual int hincrbyfloat(const Context &ctx, const Bytes &name,const Bytes &key, long double by, long double *new_val);
	//int multi_hset(const Context &ctx, const Bytes &name,const std::vector<Bytes> &kvs, int offset=0);
	//int multi_hdel(const Context &ctx, const Bytes &name,const std::vector<Bytes> &keys, int offset=0);

	virtual int hsize(const Context &ctx, const Bytes &name,uint64_t *size);
	virtual int hmget(const Context &ctx, const Bytes &name,const std::vector<std::string> &reqKeys, std::map<std::string, std::string> &val);
	virtual int hgetall(const Context &ctx, const Bytes &name,std::map<std::string, std::string> &val);
	virtual int hget(const Context &ctx, const Bytes &name,const Bytes &key, std::pair<std::string, bool> &val);
//	virtual HIterator* hscan(const Context &ctx, const Bytes &name,const Bytes &start, const Bytes &end, uint64_t limit);
	virtual int hscan(const Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);


    /*  list  */
    virtual int LIndex(const Context &ctx, const Bytes &key,const int64_t index, std::pair<std::string, bool> &val);
    virtual int LLen(const Context &ctx, const Bytes &key,uint64_t *llen);
    virtual int LPop(const Context &ctx, const Bytes &key,std::pair<std::string, bool> &val);
    virtual int LPush(const Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen);
    virtual int LPushX(const Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int RPop(const Context &ctx, const Bytes &key,std::pair<std::string, bool> &val);
	virtual int RPush(const Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int RPushX(const Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen);
	virtual int LSet(const Context &ctx, const Bytes &key,const int64_t index, const Bytes &val);
	virtual int lrange(const Context &ctx, const Bytes &key,int64_t start, int64_t end, std::vector<std::string> &list);
	virtual int ltrim(const Context &ctx, const Bytes &key,int64_t start, int64_t end);



    /* set */
    virtual int sadd(const Context &ctx, const Bytes &key,const std::set<Bytes> &members, int64_t *num);
    virtual int srem(const Context &ctx, const Bytes &key,const std::vector<Bytes> &members, int64_t *num);
	virtual int scard(const Context &ctx, const Bytes &key,uint64_t *llen);
    virtual int sismember(const Context &ctx, const Bytes &key,const Bytes &member, bool *ismember);
    virtual int smembers(const Context &ctx, const Bytes &key,std::vector<std::string> &members);
	virtual int spop(const Context &ctx, const Bytes &key,std::vector<std::string> &members, int64_t popcnt);
	virtual int srandmember(const Context &ctx, const Bytes &key,std::vector<std::string> &members, int64_t cnt);
	virtual int sscan(const Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);

	/* zset */
	virtual int multi_zset(const Context &ctx, const Bytes &name,const std::map<Bytes ,Bytes> &sortedSet, int flags, int64_t *count);
	virtual int multi_zdel(const Context &ctx, const Bytes &name,const std::set<Bytes> &keys, int64_t *count);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Context &ctx, const Bytes &name,const Bytes &key, double by, int &flags, double *new_val);
	//int multi_zset(const Context &ctx, const Bytes &name,const std::vector<Bytes> &kvs, int offset=0);
	//int multi_zdel(const Context &ctx, const Bytes &name,const std::vector<Bytes> &keys, int offset=0);

	int zremrangebyscore(const Context &ctx, const Bytes &name,const Bytes &score_start, const Bytes &score_end, bool remove, int64_t *count);

	virtual int zsize(const Context &ctx, const Bytes &name,uint64_t *size);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Context &ctx, const Bytes &name,const Bytes &key, double *score);
	virtual int zrank(const Context &ctx, const Bytes &name,const Bytes &key, int64_t *rank);
	virtual int zrrank(const Context &ctx, const Bytes &name,const Bytes &key, int64_t *rank);
	virtual int zrange(const Context &ctx, const Bytes &name,const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score);
	virtual int zrrange(const Context &ctx, const Bytes &name,const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score);
    virtual int zrangebyscore(const Context &ctx, const Bytes &name,const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                int withscores, long offset, long limit);
    virtual int zrevrangebyscore(const Context &ctx, const Bytes &name,const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                int withscores, long offset, long limit);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual int zscan(const Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);

	virtual int zlexcount(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, int64_t *count);
    virtual int zrangebylex(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							long offset, long limit);
    virtual int zrevrangebylex(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							   long offset, long limit);
    virtual int zremrangebylex(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, int64_t *count);


	/* eset */
	virtual int eset(const Context &ctx, const Bytes &key,int64_t ts);
	virtual int edel(const Context &ctx, const Bytes &key);
	virtual int eget(const Context &ctx, const Bytes &key,int64_t *ts);
    virtual int check_meta_key(const Context &ctx, const Bytes &key);

	virtual int redisCursorCleanup();


	void updateRecievedInfo(uint64_t timestamp, uint64_t index) {
		SSDBImpl::recievedTimestamp = timestamp;
		SSDBImpl::recievedIndex = index;
	}

	void updateCommitedInfo(uint64_t timestamp, uint64_t index) {
		SSDBImpl::commitedTimestamp = timestamp;
		SSDBImpl::commitedIndex = index;
	}

	void resetRecievedInfo() {
		SSDBImpl::recievedTimestamp = 0;
		SSDBImpl::recievedIndex = 0;
	}

	std::pair<uint64_t, uint64_t> getCommitedInfo() {
		std::pair<uint64_t, uint64_t> res;
		res.first = SSDBImpl::commitedTimestamp;
		res.second = SSDBImpl::commitedIndex;
		return res;
	}

private:

	int SetGeneric(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const Bytes &val, int flags, const int64_t expire, int *added);
    int GetKvMetaVal(const std::string &meta_key, KvMetaVal &kv);

    int del_key_internal(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch);
    int mark_key_deleted(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const std::string &meta_key, std::string &meta_val);

	int edel_one(const Context &ctx, const Bytes &key,leveldb::WriteBatch &batch);


	int GetHashMetaVal(const std::string &meta_key, HashMetaVal &hv);
    int GetHashItemValInternal(const std::string &item_key, std::string *val);
    HIterator* hscan_internal(const Context &ctx, const Bytes &name,const Bytes &start, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);
    int incr_hsize(const Context &ctx, const Bytes &name, leveldb::WriteBatch &batch, const std::string &size_key, HashMetaVal &hv, int64_t incr);
    int hset_one(leveldb::WriteBatch &batch, const HashMetaVal &hv, bool check_exists, const Bytes &name, const Bytes &key, const Bytes &val);
    int GetSetMetaVal(const std::string &meta_key, SetMetaVal &sv);
    int GetSetItemValInternal(const std::string &item_key);
    SIterator* sscan_internal(const Context &ctx, const Bytes &name,const Bytes &start, uint16_t version, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr);
    int incr_ssize(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const SetMetaVal &sv, const std::string &meta_key,int64_t incr);

    int GetListItemValInternal(const std::string &item_key, std::string *val, const leveldb::ReadOptions &options = leveldb::ReadOptions());
    int GetListMetaVal(const std::string &meta_key, ListMetaVal &lv);
    int doListPop(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, ListMetaVal &lv, std::string &meta_key, LIST_POSITION lp, std::pair<std::string, bool> &val);

	template <typename T>
	int doListPush(const Context &ctx, const Bytes &key, leveldb::WriteBatch &batch, const std::vector<T> &val, int offset, std::string &meta_key, ListMetaVal &meta_val, LIST_POSITION lp);

    int GetZSetMetaVal(const std::string &meta_key, ZSetMetaVal &zv);
	int GetZSetItemVal(const std::string &item_key, double *score);

	ZIterator* zscan_internal(const Context &ctx, const Bytes &name,const Bytes &score_start, const Bytes &score_end,
										uint64_t limit, Iterator::Direction direction, uint16_t version,
										const leveldb::Snapshot *snapshot=nullptr);
    ZIteratorByLex* zscanbylex_internal(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end,
							  uint64_t limit, Iterator::Direction direction, uint16_t version,
							  const leveldb::Snapshot *snapshot=nullptr);
	int	zset_one(leveldb::WriteBatch &batch, bool needCheck, const Bytes &name, const Bytes &key, double score, uint16_t cur_version, int *flags, double *newscore);
	int zdel_one(leveldb::WriteBatch &batch, const Bytes &name, const Bytes &key, uint16_t version);
	int incr_zsize(const Context &ctx, const Bytes &name, leveldb::WriteBatch &batch, const ZSetMetaVal &zv,int64_t incr);

	int setNoLock(const Context &ctx, const Bytes &key,const Bytes &val, int flags, int *added);

    template <typename T>
    int saddNoLock(const Context &ctx, const Bytes &key,const std::set<T> &mem_set, int64_t *num);

	template <typename T>
	int hmsetNoLock(const Context &ctx, const Bytes &name,const std::map<T,T> &kvs, bool check_exists);

	template <typename T>
	int zsetNoLock(const Context &ctx, const Bytes &name,const std::map<T ,T> &sortedSet, int flags, int64_t *num);

	int rpushNoLock(const Context &ctx, const Bytes &key,const std::vector<std::string> &val, int offset, uint64_t *llen);

	int zdelNoLock(const Context &ctx, const Bytes &name,const std::set<Bytes> &keys, int64_t *count);
    int zrangeGeneric(const Context &ctx, const Bytes &name,const Bytes &begin, const Bytes &limit, std::vector<string> &key_score, int reverse);
    int genericZrangebyscore(const Context &ctx, const Bytes &name,const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
                             int withscores, long offset, long limit, int reverse);
    int genericZrangebylex(const Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, std::vector<string> &keys,
						   long offset, long limit, int save, int64_t *count);

private:
	//    pthread_mutex_t mutex_bgtask_;
	Mutex mutex_bgtask_;
	std::atomic<bool> bgtask_flag_;
	pthread_t bg_tid_;
    std::queue<std::string> tasks_;
	CondVar bg_cv_;
    RecordMutex<Mutex> mutex_record_;

	void load_delete_keys_from_db(int num);
    void delete_key_loop(const std::string& del_key);
    int  delete_meta_key(const DeleteKey& dk, leveldb::WriteBatch& batch);
	void runBGTask();
	static void* thread_func(void *arg);

public:
    void start();
    void stop();
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
