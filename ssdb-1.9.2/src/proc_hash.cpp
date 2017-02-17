/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* hash */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_hexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	const Bytes &key = req[2];
	std::string val;
	int ret = serv->ssdb->hget(name, key, &val);
	resp->reply_bool(ret);
	return 0;
}


int proc_hmset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 4 || req.size() % 2 != 0){
		resp->push_back("client_error");
		resp->push_back("wrong number of arguments for 'hmset' command");
	}else{

		std::map<Bytes,Bytes> kvs;

		const Bytes &name = req[1];
		std::vector<Bytes>::const_iterator it = req.begin() + 2;
		for(; it != req.end(); it += 2){
			const Bytes &key = *it;
			const Bytes &val = *(it + 1);
			kvs[key] = val;
		}

		int ret = serv->ssdb->hmset(name, kvs);
		if(ret == -1){
			resp->push_back("error");
			return 0;
		}

		resp->reply_int(0, 1);
	}
	return 0;
}

int proc_hdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &key = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;

	std::set<Bytes> fields;
	for (int j = 2; j < req.size(); ++j) {
		fields.insert(req[j]);
	}

	int ret = serv->ssdb->hdel(key, fields);
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}

	resp->reply_int(0, ret);
	return 0;
}

int proc_hmget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	Request::const_iterator it=req.begin() + 1;
	const Bytes name = *it;
	it ++;

	std::vector<std::string> reqKeys;
	std::map<std::string, std::string> resMap;

	for(; it!=req.end(); it+=1){
		const Bytes &key = *it;
		reqKeys.push_back(key.String());
	}

	int ret = serv->ssdb->hmget(name, reqKeys, &resMap);
    if (ret == 1) {

		for (const auto& reqKey : reqKeys) {

			auto pos = resMap.find(reqKey);
			if (pos == resMap.end()) {
				//
			} else {
				resp->push_back(pos->first);
				resp->push_back(pos->second);
			}

		}

//		for(std::map<std::string, std::string>::const_iterator mit = resMap.begin();
//			mit != resMap.end(); ++mit)
//		{
//			resp->push_back(mit->first);
//			resp->push_back(mit->second);
//		}

    } else if (ret == 0) {
		//nothing
	} else {
		resp->resp.clear();
		resp->push_back("error");
	}

	return 0;
}

int proc_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	int64_t ret = serv->ssdb->hsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hset(req[1], req[2], req[3]);
	resp->reply_bool(ret);
	return 0;
}

int proc_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string val;
	int ret = serv->ssdb->hget(req[1], req[2], &val);
	resp->reply_get(ret, &val);
	return 0;
}


int proc_hgetall(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<HIterator>(serv->ssdb->hscan(req[1], "", "", -1, &snapshot));
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
		resp->push_back(it->val);
	}

	serv->ssdb->ReleaseSnapshot(snapshot);

	return 0;
}

int proc_hscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	auto it = std::unique_ptr<HIterator>(serv->ssdb->hscan(req[1], req[2], req[3], limit));

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
		resp->push_back(it->val);
	}

	return 0;
}


int proc_hkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<HIterator>(serv->ssdb->hscan(req[1], req[2], req[3], limit, &snapshot));
	it->return_val(false);

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
	}

	serv->ssdb->ReleaseSnapshot(snapshot);

	return 0;
}

int proc_hvals(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();

	const leveldb::Snapshot* snapshot = nullptr;

	auto it = std::unique_ptr<HIterator>(serv->ssdb->hscan(req[1], req[2], req[3], limit, &snapshot));

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->val);
	}

	serv->ssdb->ReleaseSnapshot(snapshot);

	return 0;
}

// dir := +1|-1
static int _hincr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(3);

	int64_t by = 1;
	if(req.size() > 3){
		by = req[3].Int64();
	}
	int64_t new_val;
	int ret = ssdb->hincr(req[1], req[2], dir * by, &new_val);
	if(ret == 0){
		resp->reply_status(-1, "value is not an integer or out of range");
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_hincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _hincr(serv->ssdb, req, resp, 1);
}