/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* zset */
#include "serv.h"


int proc_multi_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	int flags = ZADD_NONE;

	int num = 0;
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
    if(elements <= 0){
        resp->push_back("error");
        resp->push_back("ERR wrong number of arguments for 'zadd' command");
        return 0;
    } else if(elements % 2 != 0){
		//wrong args
		resp->push_back("client_error");
        resp->push_back(GetErrorInfo(SYNTAX_ERR));
		return 0;
	}
    elements /= 2;

    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;

    /* XX and NX options at the same time are not compatible. */
    if (nx && xx) {
        resp->push_back("error");
        resp->push_back("ERR XX and NX options at the same time are not compatible");
        return 0;
    }

    if (incr){
        if (incr && elements > 1){
            resp->push_back("error");
            resp->push_back("ERR INCR option supports a single increment-element pair");
            return 0;
        }

        char* eptr;
        double score = strtod(req[scoreidx+1].data(), &eptr); //check double
        if (eptr[0] != '\n' ) {  // skip for ssdb protocol
            if (eptr[0] != '\0' || errno!= 0 || std::isnan(score) || std::isinf(score)) {
                resp->push_back("error");
                resp->push_back("ERR value is not a valid float or a NaN data");
                return 0;
            }
        }else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
            resp->push_back("error");
            resp->push_back("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
            return 0;
        }

        double new_val = 0;
        int ret = serv->ssdb->zincr(name, req[scoreidx], score, flags, &new_val);
        if(ret < 0){
            resp->push_back("error");
            resp->push_back(GetErrorInfo(ret));
            return 0;
        }

        resp->reply_double(0, new_val);
        if (link->redis){
            int processed = 0;
            if (!(flags & ZADD_NOP)) processed++;
            if (processed){
                link->output->append(":");
                link->output->append(resp->resp[1].data(), resp->resp[1].size());
                link->output->append("\r\n");
            } else{
                link->output->append("$-1\r\n");
            }
            resp->resp.clear();
        }

        return 0;
    }

    std::map<Bytes,Bytes> sortedSet;

	it = req.begin() + scoreidx;
	for(; it != req.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);

 		char* eptr;
		double score = strtod(val.data(), &eptr); //check double
		if (eptr[0] != '\n' ) {  // skip for ssdb protocol
			if (eptr[0] != '\0' || errno!= 0 || std::isnan(score)) {
				resp->push_back("error");
                resp->push_back("ERR value is not a valid float");
				return 0;
			}
		}else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
            resp->push_back("error");
            resp->push_back("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
            return 0;
        }

        if (nx) {
            sortedSet.insert(make_pair(key,val));
        } else {
            sortedSet[key]=val;
        }
	}

    int ret = serv->ssdb->multi_zset(name, sortedSet, flags);
    if(ret < 0){
        resp->push_back("error");
		resp->push_back(GetErrorInfo(ret));
        return 0;
    }else{
        num += ret;
    }

    resp->reply_int(0, num);

	return 0;
}

int proc_multi_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	const Bytes &name = req[1];
    std::set<Bytes> keys;
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &key = *it;
        keys.insert(key);
	}
    int ret = serv->ssdb->multi_zdel(name, keys);
    if(ret < 0){
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

	resp->reply_int(0, ret);
	return 0;
}


int proc_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ret = serv->ssdb->zsize(req[1]);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)ret));
        return 0;
    }
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	double score = 0;
	int ret = serv->ssdb->zget(req[1], req[2], &score);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)ret));
        return 0;
    } else if(ret == 0){
        resp->push_back("not_found");
        return 0;
    } else{
		resp->reply_double(ret, score);
	}
	return 0;
}

int proc_zrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrank(req[1], req[2]);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)ret));
        return 0;
    } else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrrank(req[1], req[2]);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)ret));
        return 0;
    } else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

    resp->push_back("ok");
    int ret = serv->ssdb->zrange(req[1], req[2], req[3], resp->resp);
    if (ret < 0){
        resp->resp.clear();
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

	return 0;
}

int proc_zrrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

    resp->push_back("ok");
    int ret = serv->ssdb->zrrange(req[1], req[2], req[3], resp->resp);
    if (ret < 0){
        resp->resp.clear();
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
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
                    resp->push_back("error");
                    resp->push_back(GetErrorInfo(NAN_SCORE));
                    return 0;
                }
                pos += 3; remaining -= 3;
            } else {
                resp->push_back("error");
                resp->push_back("ERR syntax error");
                return 0;
            }
        }
    }

    resp->push_back("ok");
    if (reverse){
        ret = ssdb->zrevrangebyscore(req[1], req[2], req[3], resp->resp, withscores, offset, limit);
    } else{
        ret = ssdb->zrangebyscore(req[1], req[2], req[3], resp->resp, withscores, offset, limit);
    }
    if (ret < 0){
        resp->resp.clear();
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

    return 0;
}

int proc_zrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    return _zrangebyscore(serv->ssdb, req, resp, 0);
}

int proc_zrevrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    return _zrangebyscore(serv->ssdb, req, resp, 1);
}

int proc_zscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;


    int cursorIndex = 2;

    Bytes cursor = req[cursorIndex];

    cursor.Uint64();
    if (errno == EINVAL){
        resp->push_back("error");
        resp->push_back(GetErrorInfo(INVALID_INT));
        return 0;
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
                resp->push_back("error");
                resp->push_back(GetErrorInfo(INVALID_INT));
                return 0;
            }
        } else {
            resp->push_back("error");
            resp->push_back(GetErrorInfo(SYNTAX_ERR));
            return 0;
        }
    }
    resp->push_back("ok");
    resp->push_back("0");

    int ret =  serv->ssdb->zscan(req[1], cursor, pattern, limit, resp->resp);
    if (ret < 0) {
        resp->resp.clear();
        resp->reply_int(-1, ret, GetErrorInfo(ret).c_str());
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
            resp->push_back("error");
            resp->push_back("ERR value is not a valid float or a NaN data");
            return 0;
        }
    } else if (score <= ZSET_SCORE_MIN || score >= ZSET_SCORE_MAX){
        resp->push_back("error");
        resp->push_back("ERR value is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX");
        return 0;
    }

    double new_val = 0;
    int ret = ssdb->zincr(req[1], req[2], dir * score, flags, &new_val);
    if(ret < 0){
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

    resp->reply_double(0, new_val);
    if (link->redis){
        int processed = 0;
        if (!(flags & ZADD_NOP)) processed++;
        if (processed){
            const std::string &val = resp->resp[1];
            char buf[32];
            snprintf(buf, sizeof(buf), "$%d\r\n", (int)val.size());
            link->output->append(buf);
            link->output->append(val.data(), val.size());
            link->output->append("\r\n");
        } else{
            link->output->append("$-1\r\n");
        }
        resp->resp.clear();
    }

	return 0;
}

int proc_zincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, link, req, resp, 1);
}

int proc_zdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, link, req, resp, -1);
}

int proc_zcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(4);

    int64_t count = serv->ssdb->zremrangebyscore(req[1], req[2], req[3], 0);

    if (count < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)count));
        return 0;
    }

    resp->reply_int(0, count);
    return 0;
}

int proc_zremrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

 	int64_t count = serv->ssdb->zremrangebyscore(req[1], req[2], req[3], 1);

	if (count < 0) {
		resp->push_back("error");
        resp->push_back(GetErrorInfo((int)count));
		return 0;
	}

	resp->reply_int(0, count);
	return 0;
}

int proc_zremrangebyrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

    std::vector<std::string> key_score;
    int ret = serv->ssdb->zrange(req[1], req[2], req[3], key_score);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    } else if (ret == 0) {
        resp->reply_int(0, ret);
        return 0;
    }

    std::set<Bytes>  keys;
    for (int i = 0; i < key_score.size(); i += 2) {
        keys.insert(key_score[i]);
    }

    ret = serv->ssdb->multi_zdel(req[1], keys);
    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

	resp->reply_int(0, ret);

	return 0;
}

int proc_zfix(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &name = req[1];
	int64_t ret = serv->ssdb->zfix(name);
	resp->reply_int(ret, ret);
	return 0;
}

static int _zrangebylex(SSDB *ssdb, const Request &req, Response *resp, int reverse){
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
                    resp->push_back("error");
                    resp->push_back(GetErrorInfo(NAN_SCORE));
                    return 0;
                }
                pos += 3; remaining -= 3;
            } else {
                resp->push_back("error");
                resp->push_back("ERR syntax error");
                return 0;
            }
        }
    }

    resp->push_back("ok");
    if (reverse){
        ret = ssdb->zrevrangebylex(req[1], req[2], req[3], resp->resp, offset, limit);
    } else{
        ret = ssdb->zrangebylex(req[1], req[2], req[3], resp->resp, offset, limit);
    }
    if (ret < 0){
        resp->resp.clear();
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    }

    return 0;
}

int proc_zrangebylex(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    return _zrangebylex(serv->ssdb, req, resp, 0);
}

int proc_zremrangebylex(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(4);

    int64_t count = serv->ssdb->zremrangebylex(req[1], req[2], req[3]);

    if (count < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo((int)count));
        return 0;
    }

    resp->reply_int(0, count);
    return 0;
}

int proc_zrevrangebylex(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    return _zrangebylex(serv->ssdb, req, resp, 1);
}

int proc_zlexcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(4);
    SSDBServer *serv = (SSDBServer *)net->data;

    int64_t count = serv->ssdb->zlexcount(req[1], req[2], req[3]);
    if (count < 0){
        resp->push_back("error");
        resp->push_back(GetErrorInfo(SYNTAX_ERR));
        return 0;
    }

    resp->reply_int(0, count);

    return 0;
}
