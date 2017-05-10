/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* zset */
#include "serv.h"


int proc_multi_zset(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(4);

    SSDBServer *serv = (SSDBServer *) ctx.net->data;
	int flags = ZADD_NONE;

    int elements;
	const Bytes &name = req[1];

	int scoreidx = 2;
	std::vector<Bytes>::const_iterator it = req.begin() + scoreidx;
	for(; it != req.end(); it += 1){
        std::string key = (*it).String();
        strtolower(&key);

		if (key=="nx") {
            flags |= ZADD_NX;
        } else if (key=="xx") {
            flags |= ZADD_XX;
        } else if (key=="ch") {
            flags |= ZADD_CH;
        } else if (key=="incr") {
            flags |= ZADD_INCR;
        } else
            break;
        scoreidx++;
    }

    elements = (int)req.size() - scoreidx;
    if(elements < 0){
        reply_errinfo_return("ERR wrong number of arguments for 'zadd' command");
    } else if((elements == 0) | (elements % 2 != 0)){
		//wrong args
        reply_err_return(SYNTAX_ERR);
    }
    elements /= 2;

    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;

    /* XX and NX options at the same time are not compatible. */
    if (nx && xx) {
        reply_errinfo_return("ERR XX and NX options at the same time are not compatible");
    }

    if (incr){
        if (incr && elements > 1){
            reply_errinfo_return("ERR INCR option supports a single increment-element pair");
        }

        char* eptr;
        double score = strtod(req[scoreidx].data(), &eptr); //check double
        if (eptr[0] != '\n' ) {  // skip for ssdb protocol
            if (eptr[0] != '\0' || errno!= 0 || std::isnan(score) || std::isinf(score)) {
                reply_errinfo_return("ERR value is not a valid float or a NaN data");
            }
        }else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
            reply_errinfo_return("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
        }

        double new_val = 0;
        int ret = serv->ssdb->zincr(name, req[scoreidx+1], score, flags, &new_val);
        check_key(ret);
        if (ret < 0){
            reply_err_return(ret);
        }

        resp->reply_double(0, new_val);

        {
            int processed = 0;
            if (!(flags & ZADD_NOP)) processed++;
            if (processed){
                resp->redisResponse = new RedisResponse(resp->resp[1]);
            } else{
                resp->redisResponse = new RedisResponse("");
            }
        }


        return 0;
    }

    std::map<Bytes,Bytes> sortedSet;

	it = req.begin() + scoreidx;
	for(; it != req.end(); it += 2){
		const Bytes &key = *(it + 1);
		const Bytes &val = *it;

 		char* eptr;
		double score = strtod(val.data(), &eptr); //check double
		if (eptr[0] != '\n' ) {  // skip for ssdb protocol
			if (eptr[0] != '\0' || errno!= 0 || std::isnan(score)) {
                reply_errinfo_return("ERR value is not a valid float");
			}
		}else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
            reply_errinfo_return("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
        }

        if (nx) {
            sortedSet.insert(make_pair(key,val));
        } else {
            sortedSet[key]=val;
        }
	}

    int64_t num = 0;
    int ret = serv->ssdb->multi_zset(name, sortedSet, flags, &num);
    check_key(ret);
    if (ret < 0){
        reply_err_return(ret);
    }

    resp->reply_int(0, num);

	return 0;
}

int proc_multi_zdel(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	const Bytes &name = req[1];
    std::set<Bytes> keys;
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &key = *it;
        keys.insert(key);
	}

    int64_t count = 0;
    int ret = serv->ssdb->multi_zdel(name, keys, &count);
    check_key(ret);
    if (ret < 0){
        reply_err_return(ret);
    }

	resp->reply_int(0, count);
	return 0;
}


int proc_zsize(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

    uint64_t size = 0;
	int ret = serv->ssdb->zsize(req[1], &size);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }
	resp->reply_int(ret, size);
	return 0;
}

int proc_zget(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	double score = 0;
	int ret = serv->ssdb->zget(req[1], req[2], &score);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if(ret == 0){
        resp->reply_not_found();
        return 0;
    } else{
		resp->reply_double(ret, score);
	}
	return 0;
}

int proc_zrank(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

    int64_t rank = 0;
	int ret = serv->ssdb->zrank(req[1], req[2], &rank);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0 || rank == -1) {
        resp->reply_not_found();
    } else {
		resp->reply_int(ret, rank);
	}
	return 0;
}

int proc_zrrank(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

    int64_t rank = 0;
	int ret = serv->ssdb->zrrank(req[1], req[2], &rank);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0 || rank == -1) {
        resp->reply_not_found();
    } else{
		resp->reply_int(ret, rank);
	}
	return 0;
}

int proc_zrange(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

    resp->reply_list_ready();
    int ret = serv->ssdb->zrange(req[1], req[2], req[3], resp->resp);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

	return 0;
}

int proc_zrrange(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

    resp->reply_list_ready();
    int ret = serv->ssdb->zrrange(req[1], req[2], req[3], resp->resp);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

	return 0;
}

int string2ld(const char *s, size_t slen, long *value) {
    long long lvalue;
    if (string2ll(s,slen,&lvalue) == 0)
        return NAN_SCORE;
    if (lvalue < LONG_MIN || lvalue > LONG_MAX) {
        return NAN_SCORE;
    }
    if (value) *value = lvalue;

    return 1;
}

static int _zrangebyscore(SSDB *ssdb, const Request &req, Response *resp, int reverse){
    CHECK_NUM_PARAMS(4);
    long offset = 0, limit = -1;
    int withscores = 0;
    int ret = 0;

    if (req.size() > 4) {
        int remaining = req.size() - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 1 && !strcasecmp(req[pos].data(),"withscores")) {
                pos++; remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(req[pos].data(),"limit")) {
                if ( (string2ld(req[pos+1].data(),req[pos+1].size(),&offset) < 0) ||
                     (string2ld(req[pos+2].data(),req[pos+2].size(),&limit) < 0) ){
                    reply_err_return(NAN_SCORE);
                }
                pos += 3; remaining -= 3;
            } else {
                reply_err_return(SYNTAX_ERR);
            }
        }
    }

    resp->reply_list_ready();
    if (reverse){
        ret = ssdb->zrevrangebyscore(req[1], req[2], req[3], resp->resp, withscores, offset, limit);
    } else{
        ret = ssdb->zrangebyscore(req[1], req[2], req[3], resp->resp, withscores, offset, limit);
    }
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}

int proc_zrangebyscore(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    return _zrangebyscore(serv->ssdb, req, resp, 0);
}

int proc_zrevrangebyscore(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    return _zrangebyscore(serv->ssdb, req, resp, 1);
}

int proc_zscan(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    int cursorIndex = 2;

    Bytes cursor = req[cursorIndex];

    cursor.Uint64();
    if (errno == EINVAL){
        reply_err_return(INVALID_INT);
    }

    std::string pattern = "*";
    uint64_t limit = 10;

    std::vector<Bytes>::const_iterator it = req.begin() + cursorIndex + 1;
    for(; it != req.end(); it += 2){
        std::string key = (*it).String();
        strtolower(&key);

        if (key=="match") {
            pattern = (*(it+1)).String();
        } else if (key=="count") {
            limit =  (*(it+1)).Uint64();
            if (errno == EINVAL){
                reply_err_return(INVALID_INT);
            }
        } else {
            reply_err_return(SYNTAX_ERR);
        }
    }
    resp->reply_scan_ready();

    int ret =  serv->ssdb->zscan(req[1], cursor, pattern, limit, resp->resp);
    check_key(ret);
    if (ret < 0) {
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {
    }

    return 0;
}


// dir := +1|-1
static int _zincr(SSDB *ssdb, Link *link, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(4);
    int flags = ZADD_NONE;
    flags |= ZADD_INCR;

    char* eptr;
    double score = strtod(req[3].data(), &eptr); //check double
    if (eptr[0] != '\n' ) {  // skip for ssdb protocol
        if (eptr[0] != '\0' || errno!= 0 || std::isnan(score) || std::isinf(score)) {
            reply_errinfo_return("ERR value is not a valid float or a NaN data");
        }
    } else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
        reply_errinfo_return("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
    }

    double new_val = 0;
    int ret = ssdb->zincr(req[1], req[2], dir * score, flags, &new_val);
    if (ret < 0){
        reply_err_return(ret);
    }

    resp->reply_double(0, new_val);

    {
        int processed = 0;
        if (!(flags & ZADD_NOP)) processed++;
        if (processed){
            resp->redisResponse = new RedisResponse(resp->resp[1]);
        } else{
            resp->redisResponse = new RedisResponse("");
        }
    }

	return 0;
}

int proc_zincr(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	return _zincr(serv->ssdb, link, req, resp, 1);
}

int proc_zdecr(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	return _zincr(serv->ssdb, link, req, resp, -1);
}

int proc_zcount(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(4);

    int64_t count = 0;
    int ret = serv->ssdb->zremrangebyscore(req[1], req[2], req[3], false, &count);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }

    resp->reply_int(0, count);
    return 0;
}

int proc_zremrangebyscore(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

 	int64_t count = 0;
    int ret = serv->ssdb->zremrangebyscore(req[1], req[2], req[3], true, &count);
	check_key(ret);
    if (ret < 0) {
		reply_err_return(ret);
	}

	resp->reply_int(0, count);
	return 0;
}

int proc_zremrangebyrank(const Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

    std::vector<std::string> key_score;
    int ret = serv->ssdb->zrange(req[1], req[2], req[3], key_score);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_int(0, ret);
        return 0;
    }

    std::set<Bytes> keys;
    for (int i = 0; i < key_score.size(); i += 2) {
        keys.insert(Bytes(key_score[i]));
    }

    int64_t count = 0;
    ret = serv->ssdb->multi_zdel(req[1], keys, &count);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }

	resp->reply_int(0, count);

	return 0;
}


static int _zrangebylex(SSDB *ssdb, const Request &req, Response *resp, enum DIRECTION direction){
    CHECK_NUM_PARAMS(4);
    long offset = 0, limit = -1;
    int ret = 0;

    if (req.size() > 4) {
        int remaining = req.size() - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 3 && !strcasecmp(req[pos].data(),"limit")) {
                if ( (string2ld(req[pos+1].data(),req[pos+1].size(),&offset) < 0) ||
                     (string2ld(req[pos+2].data(),req[pos+2].size(),&limit) < 0) ){
                    reply_err_return(NAN_SCORE);
                }
                pos += 3; remaining -= 3;
            } else {
                reply_err_return(SYNTAX_ERR);
            }
        }
    }

    resp->reply_list_ready();
    if (direction == DIRECTION::BACKWARD){
        ret = ssdb->zrevrangebylex(req[1], req[2], req[3], resp->resp, offset, limit);
    } else{
        ret = ssdb->zrangebylex(req[1], req[2], req[3], resp->resp, offset, limit);
    }
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}

int proc_zrangebylex(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    return _zrangebylex(serv->ssdb, req, resp, DIRECTION::FORWARD);
}

int proc_zremrangebylex(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(4);

    int64_t count = 0;

    int ret = serv->ssdb->zremrangebylex(req[1], req[2], req[3], &count);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }

    resp->reply_int(0, count);

    return 0;
}

int proc_zrevrangebylex(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    return _zrangebylex(serv->ssdb, req, resp, DIRECTION::BACKWARD);
}

int proc_zlexcount(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(4);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    int64_t count = 0;

    int ret = serv->ssdb->zlexcount(req[1], req[2], req[3], &count);
    check_key(ret);
    if (ret < 0){
        reply_err_return(SYNTAX_ERR);
    }

    resp->reply_int(0, count);

    return 0;
}
