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

	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Bytes &key, const Bytes &val) = 0;
	virtual int raw_del(const Bytes &key) = 0;
	virtual int raw_get(const Bytes &key, std::string *val) = 0;

	/* 	General	*/
	virtual int type(const Bytes &key, std::string *type) = 0;
	virtual int dump(const Bytes &key, std::string *res) = 0;
    virtual int restore(const Bytes &key, int64_t expire, const Bytes &data, bool replace, std::string *res) = 0;
	virtual int exists(const Bytes &key) = 0;
    virtual int parse_replic(const std::vector<std::string> &kvs) = 0;

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val, int flags) = 0;
	virtual int del(const Bytes &key) = 0;
	virtual int append(const Bytes &key, const Bytes &value, uint64_t *new_len) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val) = 0;
	virtual int incrbyfloat(const Bytes &key, long double by, long double *new_val) = 0;
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0) = 0;
	virtual int multi_del(const std::vector<Bytes> &keys, int offset=0) = 0;
	virtual int setbit(const Bytes &key, int64_t bitoffset, int on, int *res) = 0;
	virtual int getbit(const Bytes &key, int64_t bitoffset, int *res) = 0;
	
	virtual int get(const Bytes &key, std::string *val) = 0;
	virtual int getset(const Bytes &key, std::pair<std::string, bool> &val, const Bytes &newval) = 0;
	virtual int getrange(const Bytes &key, int64_t start, int64_t end, std::pair<std::string, bool> &res) = 0;
	virtual int setrange(const Bytes &key, int64_t start, const Bytes &value, uint64_t *new_len) = 0;

	virtual int scan(const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	// return (start, end]

	/* hash */

	virtual int hmset(const Bytes &name, const std::map<Bytes, Bytes> &kvs) = 0;
	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val, int *added) = 0;
	virtual int hsetnx(const Bytes &name, const Bytes &key, const Bytes &val, int *added) = 0;
	virtual int hdel(const Bytes &name, const std::set<Bytes>& fields, int *deleted) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val) = 0;
	virtual int hincrbyfloat(const Bytes &name, const Bytes &key, long double by, long double *new_val) = 0;

	virtual int hsize(const Bytes &name, uint64_t *size) = 0;
	virtual int hget(const Bytes &name, const Bytes &key, std::pair<std::string, bool> &val) = 0;
	virtual int hgetall(const Bytes &name, std::map<std::string, std::string> &val) = 0;
	virtual int hmget(const Bytes &name, const std::vector<std::string> &reqKeys, std::map<std::string, std::string> &val) = 0;
	virtual int hscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	/*  list  */
	virtual int LIndex(const Bytes &key, const int64_t index, std::pair<std::string, bool> &val) = 0;
	virtual int LLen(const Bytes &key, uint64_t *llen) = 0;
	virtual int LPop(const Bytes &key, std::pair<std::string, bool> &val) = 0;
	virtual int LPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int LPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int RPop(const Bytes &key, std::pair<std::string, bool> &val) = 0;
	virtual int RPush(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int RPushX(const Bytes &key, const std::vector<Bytes> &val, int offset, uint64_t *llen) = 0;
	virtual int LSet(const Bytes &key, const int64_t index, const Bytes &val) = 0;
	virtual int lrange(const Bytes &key, int64_t start, int64_t end, std::vector<std::string> *list) = 0;
	virtual int ltrim(const Bytes &key, int64_t start, int64_t end) = 0;

	/* set */
	virtual int sadd(const Bytes &key, const std::set<Bytes> &members, int64_t *num) = 0;
    virtual int srem(const Bytes &key, const std::vector<Bytes> &members, int64_t *num) = 0;
    virtual int scard(const Bytes &key, uint64_t *llen) = 0;
	virtual int sismember(const Bytes &key, const Bytes &member, bool *ismember) = 0;
	virtual int smembers(const Bytes &key, std::vector<std::string> &members) = 0;
	virtual int spop(const Bytes &key, std::vector<std::string> &members, int64_t popcnt) = 0;
	virtual int srandmember(const Bytes &key, std::vector<std::string> &members, int64_t cnt) = 0;
	virtual int sscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	/* zset */
	virtual int multi_zset(const Bytes &name, const std::map<Bytes ,Bytes> &sortedSet, int flags) = 0;
	virtual int multi_zdel(const Bytes &name, const std::set<Bytes> &keys, int64_t *count) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &name, const Bytes &key, double by, int &flags, double *new_val) = 0;
    virtual int zsize(const Bytes &name, uint64_t *size) = 0;
    /**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, double *score) = 0;
	virtual int zrank(const Bytes &name, const Bytes &key, int64_t *rank) = 0;
	virtual int zrrank(const Bytes &name, const Bytes &key, int64_t *rank) = 0;
    virtual int zrange(const Bytes &name, const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score) = 0;
    virtual int zrrange(const Bytes &name, const Bytes &begin, const Bytes &limit, std::vector<std::string> &key_score) = 0;
    virtual int zrangebyscore(const Bytes &name, const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
				int withscores, long offset, long limit) = 0;
    virtual int zrevrangebyscore(const Bytes &name, const Bytes &start_score, const Bytes &end_score, std::vector<std::string> &key_score,
				int withscores, long offset, long limit) = 0;
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
    virtual int zscan(const Bytes &name, const Bytes& cursor, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp) = 0;

	virtual int64_t zlexcount(const Bytes &name, const Bytes &key_start, const Bytes &key_end) = 0;
    virtual int zrangebylex(const Bytes &name, const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							long offset, long limit) = 0;
    virtual int zrevrangebylex(const Bytes &name, const Bytes &key_start, const Bytes &key_end, std::vector<std::string> &keys,
							   long offset, long limit) = 0;
    virtual int64_t zremrangebylex(const Bytes &name, const Bytes &key_start, const Bytes &key_end) = 0;

	/* eset */
	virtual int eset(const Bytes &key, int64_t ts) = 0;
	virtual int edel(const Bytes &key) = 0;
	virtual int eget(const Bytes &key, int64_t *ts) = 0;
    virtual int check_meta_key(const Bytes &key) = 0;

	virtual int redisCursorCleanup() = 0;

};


template <class T>
bool doScanGeneric(const T &mit, const std::string &pattern, uint64_t limit, std::vector<std::string> &resp);


#endif
