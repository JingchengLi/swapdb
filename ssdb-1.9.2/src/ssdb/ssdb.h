/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_H_
#define SSDB_H_

#include <vector>
#include <set>
#include <string>
#include <map>
#include <util/sorted_set.h>
#include <memory>
#include "const.h"
#include "options.h"
#include "iterator.h"
#include "internal_error.h"
#include "t_scan.h"

class Bytes;
class Config;

class SSDB{
public:
	SSDB(){}
	virtual ~SSDB(){};
	static SSDB* open(const Options &opt, const std::string &base_dir);
	
	virtual int flushdb() = 0;

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr) = 0;
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=nullptr) = 0;

	//void flushdb();
	virtual uint64_t size() = 0;
	virtual std::vector<std::string> info() = 0;
	virtual void compact() = 0;
	virtual int digest(std::string *val) = 0;

	/* raw operates */

	// repl: whether to sync this operation to slaves

	virtual int raw_set(Context &ctx, const Bytes &key,const Bytes &val) = 0;
	virtual int raw_del(Context &ctx, const Bytes &key) = 0;
	virtual int raw_get(Context &ctx, const Bytes &key, std::string *val) = 0;

	/* 	General	*/
	virtual int type(Context &ctx, const Bytes &key,std::string *type) = 0;
	virtual int dump(Context &ctx, const Bytes &key,std::string *res) = 0;
    virtual int restore(Context &ctx, const Bytes &key,int64_t expire, const Bytes &data, bool replace, std::string *res) = 0;
	virtual int exists(Context &ctx, const Bytes &key) = 0;
    virtual int parse_replic(Context &ctx, const std::vector<std::string> &kvs) = 0;

	/* key value */

	virtual int set(Context &ctx, const Bytes &key,const Bytes &val, int flags, int *added) = 0;
	virtual int del(Context &ctx, const Bytes &key) = 0;
	virtual int append(Context &ctx, const Bytes &key,const Bytes &value, uint64_t *new_len) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(Context &ctx, const Bytes &key,int64_t by, int64_t *new_val) = 0;
	virtual int incrbyfloat(Context &ctx, const Bytes &key,long double by, long double *new_val) = 0;
	virtual int multi_set(Context &ctx, const std::vector<Bytes> &kvs, int offset=0) = 0;
	virtual int multi_del(Context &ctx, const std::set<Bytes> &keys, int64_t *count) = 0;
	virtual int setbit(Context &ctx, const Bytes &key,int64_t bitoffset, int on, int *res) = 0;
	virtual int getbit(Context &ctx, const Bytes &key,int64_t bitoffset, int *res) = 0;
	
	virtual int get(Context &ctx, const Bytes &key,std::string *val) = 0;
	virtual int getset(Context &ctx, const Bytes &key,std::pair<std::string, bool> &val, const Bytes &newval) = 0;
	virtual int getrange(Context &ctx, const Bytes &key,int64_t start, int64_t end, std::pair<std::string, bool> &res) = 0;
	virtual int setrange(Context &ctx, const Bytes &key,int64_t start, const Bytes &value, uint64_t *new_len) = 0;

	virtual int scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	// return (start, end]

	/* hash */

	virtual int hmset(Context &ctx, const Bytes &name,const std::map<Bytes, Bytes> &kvs) = 0;
	virtual int hset(Context &ctx, const Bytes &name,const Bytes &key, const Bytes &val, int *added) = 0;
	virtual int hsetnx(Context &ctx, const Bytes &name,const Bytes &key, const Bytes &val, int *added) = 0;
	virtual int hdel(Context &ctx, const Bytes &name,const std::set<Bytes>& fields, int *deleted) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(Context &ctx, const Bytes &name,const Bytes &key, int64_t by, int64_t *new_val) = 0;
	virtual int hincrbyfloat(Context &ctx, const Bytes &name,const Bytes &key, long double by, long double *new_val) = 0;

	virtual int hsize(Context &ctx, const Bytes &name,uint64_t *size) = 0;
	virtual int hget(Context &ctx, const Bytes &name,const Bytes &key, std::pair<std::string, bool> &val) = 0;
	virtual int hgetall(Context &ctx, const Bytes &name,std::map<std::string, std::string> &val) = 0;
	virtual int hmget(Context &ctx, const Bytes &name,const std::vector<std::string> &reqKeys, std::map<std::string, std::string> &val) = 0;
	virtual int hscan(Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	/*  list  */
	virtual int LIndex(Context &ctx, const Bytes &key,const int64_t index, std::pair<std::string, bool> &val) = 0;
	virtual int LLen(Context &ctx, const Bytes &key,uint64_t *llen) = 0;
	virtual int LPop(Context &ctx, const Bytes &key,std::pair<std::string, bool> &val) = 0;
	virtual int LPush(Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int LPushX(Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int RPop(Context &ctx, const Bytes &key,std::pair<std::string, bool> &val) = 0;
	virtual int RPush(Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int RPushX(Context &ctx, const Bytes &key,const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int LSet(Context &ctx, const Bytes &key,const int64_t index, const Bytes &val) = 0;
	virtual int lrange(Context &ctx, const Bytes &key,int64_t start, int64_t end, std::vector<std::string> &list) = 0;
	virtual int ltrim(Context &ctx, const Bytes &key,int64_t start, int64_t end) = 0;

	/* set */
	virtual int sadd(Context &ctx, const Bytes &key,const std::set<Bytes> &members, int64_t *num) = 0;
    virtual int srem(Context &ctx, const Bytes &key,const std::vector<Bytes> &members, int64_t *num) = 0;
    virtual int scard(Context &ctx, const Bytes &key,uint64_t *llen) = 0;
	virtual int sismember(Context &ctx, const Bytes &key,const Bytes &member, bool *ismember) = 0;
	virtual int smembers(Context &ctx, const Bytes &key,std::vector<std::string> &members) = 0;
	virtual int spop(Context &ctx, const Bytes &key,std::vector<std::string> &members, int64_t popcnt) = 0;
	virtual int srandmember(Context &ctx, const Bytes &key,std::vector<std::string> &members, int64_t cnt) = 0;
	virtual int sscan(Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	/* zset */
	virtual int multi_zset(Context &ctx, const Bytes &name,const std::map<Bytes ,Bytes> &sortedSet, int flags, int64_t *num) = 0;
	virtual int multi_zdel(Context &ctx, const Bytes &name,const std::set<Bytes> &keys, int64_t *count) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(Context &ctx, const Bytes &name,const Bytes &key, double by, int &flags, double *new_val) = 0;
    virtual int zsize(Context &ctx, const Bytes &name,uint64_t *size) = 0;
    /**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(Context &ctx, const Bytes &name,const Bytes &key, double *score) = 0;
	virtual int zrank(Context &ctx, const Bytes &name,const Bytes &key, int64_t *rank) = 0;
	virtual int zrrank(Context &ctx, const Bytes &name,const Bytes &key, int64_t *rank) = 0;
    virtual int zrange(Context &ctx, const Bytes &name,const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score) = 0;
    virtual int zrrange(Context &ctx, const Bytes &name,const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score) = 0;
    virtual int zrangebyscore(Context &ctx, const Bytes &name,const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
				int withscores, long offset, long limit) = 0;
    virtual int zrevrangebyscore(Context &ctx, const Bytes &name,const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
				int withscores, long offset, long limit) = 0;
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
    virtual int zscan(Context &ctx, const Bytes &name,const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	virtual int zlexcount(Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, int64_t *count) = 0;
    virtual int zrangebylex(Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							long offset, long limit) = 0;
    virtual int zrevrangebylex(Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							   long offset, long limit) = 0;
    virtual int zremrangebylex(Context &ctx, const Bytes &name,const Bytes &key_start, const Bytes &key_end, int64_t *count) = 0;

	/* eset */
	virtual int eset(Context &ctx, const Bytes &key,int64_t ts) = 0;
	virtual int eget(Context &ctx, const Bytes &key,int64_t *ts) = 0;
    virtual int check_meta_key(Context &ctx, const Bytes &key) = 0;

	virtual int redisCursorCleanup() = 0;

};

//
//template <class T>
//bool doScanGeneric(const T &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);


#endif
