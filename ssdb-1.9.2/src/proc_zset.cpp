/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* zset */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	const Bytes &name = req[1];
	const Bytes &key = req[2];
	double val = 0;
	int ret = serv->ssdb->zget(name, key, &val);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	resp->push_back("ok");
	const Bytes &name = req[1];
	double val = 0;
	for(Request::const_iterator it=req.begin()+2; it!=req.end(); it++){
		const Bytes &key = *it;
		int ret = serv->ssdb->zget(name, key, &val);
		resp->push_back(key.String());
		if(ret > 0){
			resp->push_back("1");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
}

int proc_multi_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->zsize(key);
		resp->push_back(key.String());
		if(ret == -1){
			resp->push_back("-1");
		}else{
			resp->add(ret);
		}
	}
	return 0;
}

int proc_multi_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	int flags = ZADD_NONE;

	if(req.size() < 4){
		resp->push_back("client_error");
		return 0;
	}

	int num = 0;
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
        }

        if (key=="nx" || key=="xx" || key=="ch" || key=="incr") {
            scoreidx++;
        } else break;

    }

	if((req.size() - 2 + scoreidx) % 2 != 0){
		//wrong args
		resp->push_back("client_error");
		return 0;
	}

    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;

    /* XX and NX options at the same time are not compatible. */
    if (nx && xx) {
        resp->push_back("error");
        return 0;
    }

    std::map<Bytes,Bytes> sortedSet;

	it = req.begin() + scoreidx;
	for(; it != req.end(); it += 2){
		const Bytes &key = *it;
		const Bytes &val = *(it + 1);

 		char* eptr;
		double score = strtod(val.data(), &eptr); //check double
		if (eptr[0] != '\0' || errno!= 0 || std::isnan(score)) {
			resp->push_back("error");
			return 0;
		}

        if (nx) {
            sortedSet.insert(make_pair(key,val));
        } else {
            sortedSet[key]=val;
        }
	}

    //INCR option supports a single increment-element pair
    if (incr && sortedSet.size() > 1) {
        resp->push_back("error");
        return 0;
    }

    int ret = serv->ssdb->multi_zset(name, sortedSet, flags);
    if(ret == -1){
        resp->push_back("error");
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

	int num = 0;
	const Bytes &name = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &key = *it;
		int ret = serv->ssdb->zdel(name, key);
		if(ret == -1){
			resp->push_back("error");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	resp->push_back("ok");
	Request::const_iterator it=req.begin() + 1;
	const Bytes name = *it;
	it ++;
	for(; it!=req.end(); it+=1){
		const Bytes &key = *it;
		double score = 0;
		int ret = serv->ssdb->zget(name, key, &score);
		if(ret == 1){
			resp->push_back(key.String());
			resp->push_back(str(score));
		}
	}
	return 0;
}

int proc_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int ret = serv->ssdb->zset(req[1], req[2], req[3]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ret = serv->ssdb->zsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	double score = 0;
	int ret = serv->ssdb->zget(req[1], req[2], &score);
	if(ret == 0){
		resp->add("not_found");
	}else{
		resp->reply_double(ret, score);
	}
	return 0;
}

int proc_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int ret = serv->ssdb->zdel(req[1], req[2]);
	resp->reply_bool(ret);
	return 0;
}

int proc_zrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrank(req[1], req[2]);
	if(ret == -1){
		resp->add("not_found");
	}else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrrank(req[1], req[2]);
	if(ret == -1){
		resp->add("not_found");
	}else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t offset = req[2].Int64();
	int64_t limit = req[3].Int64();

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<ZIterator>(serv->ssdb->zrange(req[1], offset, limit, &snapshot));
	resp->push_back("ok");

    while(it->next()){
        resp->push_back(it->key);
        resp->push_back(str(it->score));
    }

	serv->ssdb->ReleaseSnapshot(snapshot);

	return 0;
}

int proc_zrrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t offset = req[2].Uint64();
	uint64_t limit = req[3].Uint64();

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<ZIterator>(serv->ssdb->zrrange(req[1], offset, limit, &snapshot));
	resp->push_back("ok");

    while(it->next()){
        resp->push_back(it->key);
        resp->push_back(str(it->score));
    }

	serv->ssdb->ReleaseSnapshot(snapshot);

	return 0;
}

int proc_zscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	auto it = std::unique_ptr<ZIterator>(serv->ssdb->zscan(req[1], req[2], req[3], req[4], limit));
	if(offset > 0){
		it->skip(offset);
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
		resp->push_back(str(it->score));
	}
	return 0;
}

int proc_zkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	uint64_t limit = req[5].Uint64();
	auto it = std::unique_ptr<ZIterator>(serv->ssdb->zscan(req[1], req[2], req[3], req[4], limit));
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
	}

	return 0;
}


// dir := +1|-1
static int _zincr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(3);

	double by = 1;
	if(req.size() > 3){
		by = req[3].Double();
	}
	double new_val;
	int ret = ssdb->zincr(req[1], req[2], dir * by, &new_val);
	resp->reply_double(ret, new_val);
	return 0;
}

int proc_zincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, req, resp, 1);
}

int proc_zdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, req, resp, -1);
}

int proc_zcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t count = serv->ssdb->zcount(req[1],req[2], req[3]);

	resp->reply_int(0, count);
	return 0;
}

int proc_zremrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

 	int64_t count = serv->ssdb->zremrangebyscore(req[1], req[2], req[3]);

	if (count <0) {
		resp->push_back("error");
		return 0;
	}

	resp->reply_int(0, count);
	return 0;
}

int proc_zremrangebyrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t start = req[2].Int64();
	int64_t end = req[3].Int64();

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<ZIterator>(serv->ssdb->zrange(req[1], start, end, &snapshot));
	int64_t count = 0;
    while (it->next()) {
        count++;
        int ret = serv->ssdb->zdel(req[1], it->key);
        if (ret == -1) {
            resp->push_back("error");
			serv->ssdb->ReleaseSnapshot(snapshot);
			return 0;
        }
    }

	serv->ssdb->ReleaseSnapshot(snapshot);
	resp->reply_int(0, count);

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
