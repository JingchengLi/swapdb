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

	if (ret < 0) {
		resp->reply_bool(-1, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_bool(ret);
	}

	return 0;
}


int proc_hmset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 4 || req.size() % 2 != 0){
		resp->push_back("client_error");
		resp->push_back("wrong number of arguments for 'hmset' command");
		return 0;

	}

	std::map<Bytes,Bytes> kvs;
	const Bytes &name = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 2){
		const Bytes &key = *it;const Bytes &val = *(it + 1);
		kvs[key] = val;
	}

	int ret = serv->ssdb->hmset(name, kvs);
	if(ret < 0){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(ret));
		return 0;
	}

	resp->reply_int(0, 1);

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
	if(ret < 0){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(ret));
		return 0;
	}

	resp->reply_int(0, ret);
	return 0;
}

int proc_hmget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

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
		resp->push_back("ok");

		for (const auto& reqKey : reqKeys) {

			auto pos = resMap.find(reqKey);
			if (pos == resMap.end()) {
				//
			} else {
				resp->push_back(pos->first);
				resp->push_back(pos->second);
			}

		}

    } else if (ret == 0) {
		resp->push_back("ok");

		//nothing
	} else {
		resp->push_back("error");
		resp->push_back(GetErrorInfo(ret));
		return 0;
	}

	return 0;
}

int proc_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t size = 0;
	int ret = serv->ssdb->hsize(req[1], &size);
	if (ret < 0) {
		resp->reply_int(-1, ret, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, size);
	}

	return 0;
}

int proc_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hset(req[1], req[2], req[3]);

	if (ret < 0) {
		resp->reply_bool(-1, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_bool(ret);
	}

	return 0;
}

int proc_hsetnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hsetnx(req[1], req[2], req[3]);

	if (ret < 0) {
		resp->reply_bool(-1, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_bool(ret);
	}

	return 0;
}

int proc_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string val;
	int ret = serv->ssdb->hget(req[1], req[2], &val);

	if (ret < 0) {
		resp->reply_get(-1, &val, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}


int proc_hgetall(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;


	std::map<std::string, std::string> resMap;
	int ret = serv->ssdb->hgetall(req[1], resMap);

    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    } else if (ret == 0) {
        resp->push_back("ok");

        //nothing
    } else {
        resp->push_back("ok");

		for (const auto& res : resMap) {
			resp->push_back(res.first);
			resp->push_back(res.second);
		}

	}

	return 0;
}

int proc_hscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
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

	int ret =  serv->ssdb->hscan(req[1], cursor, pattern, limit, resp->resp);
	if (ret < 0) {
		resp->resp.clear();
		resp->reply_int(-1, ret, GetErrorInfo(ret).c_str());
	} else if (ret == 0) {
	}

	return 0;
}


int proc_hkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

//	uint64_t limit = req[4].Uint64();

    std::map<std::string, std::string> resMap;
    int ret = serv->ssdb->hgetall(req[1], resMap);

    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    } else if (ret == 0) {
        resp->push_back("ok");

        //nothing
    } else {
        resp->push_back("ok");

        for (const auto& res : resMap) {
            //TODO 这里同时处理了kv 只是没有返回.
            resp->push_back(res.first);
//            resp->push_back(res.second);
        }
    }


    return 0;
}

int proc_hvals(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

//	uint64_t limit = req[4].Uint64();

    std::map<std::string, std::string> resMap;
    int ret = serv->ssdb->hgetall(req[1], resMap);

    if (ret < 0) {
        resp->push_back("error");
        resp->push_back(GetErrorInfo(ret));
        return 0;
    } else if (ret == 0) {
        resp->push_back("ok");

        //nothing
    } else {
        resp->push_back("ok");

        for (const auto& res : resMap) {
            //TODO 这里同时处理了kv 只是没有返回.
//            resp->push_back(res.first);
            resp->push_back(res.second);
        }
    }

	return 0;
}

int proc_hincrbyfloat(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	long double by = req[3].LDouble();
	if (errno == EINVAL){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(INVALID_DBL));
		return 0;
	}

	long double new_val;
	int ret = serv->ssdb->hincrbyfloat(req[1], req[2], by, &new_val);
	if(ret  < 0){
		resp->reply_status(-1, GetErrorInfo(ret).c_str());
	} else{
		resp->reply_long_double(ret, new_val);
	}
	return 0;

}

int proc_hincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t by = req[3].Int64();

	if (errno == EINVAL){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(INVALID_INT));
		return 0;
	}

	int64_t new_val = 0;
	int ret = serv->ssdb->hincr(req[1], req[2], by, &new_val);
	if(ret < 0) {
		resp->reply_status(-1, GetErrorInfo(ret).c_str());
	} else{
		resp->reply_int(ret, new_val);
	}
	return 0;

}