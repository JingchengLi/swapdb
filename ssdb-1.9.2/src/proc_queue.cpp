/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* queue */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_qsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	uint64_t len = 0;
	int ret = serv->ssdb->LLen(req[1], &len);
	if (ret < 0) {
		resp->reply_int(-1, len, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, len);
	}

	return 0;
}


int proc_qpush_frontx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	uint64_t len = 0;
	int ret = serv->ssdb->LPushX(name, req, 2, &len);
	if (ret < 0) {
		resp->reply_int(-1, len, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, len);
	}

	return 0;
}


int proc_qpush_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
 	uint64_t len = 0;
	int ret = serv->ssdb->LPush(name, req, 2, &len);
	if (ret < 0) {
		resp->reply_int(-1, len, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, len);
	}

	return 0;
}

int proc_qpush_backx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t len = 0;
	int ret = serv->ssdb->RPushX(req[1], req, 2, &len);
	if (ret < 0) {
		resp->reply_int(-1, len, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, len);
	}

	return 0;
}


int proc_qpush_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    uint64_t len = 0;
    int ret = serv->ssdb->RPush(req[1], req, 2, &len);
	if (ret < 0) {
		resp->reply_int(-1, len, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, len);
	}

	return 0;
}


int proc_qpop_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	std::string val;
	int ret = serv->ssdb->LPop(name, &val);
	if (ret < 0) {
		resp->reply_get(-1, &val, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}

int proc_qpop_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;

    std::string val;
    int ret = serv->ssdb->RPop(req[1], &val);
	if (ret < 0) {
		resp->reply_get(-1, &val, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}


int proc_qfix(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

//	int ret = serv->ssdb->qfix(req[1]);
//	if(ret == -1){
//		resp->push_back("error");
//	}else{
		resp->push_back("ok");
//	}
	return 0;
}


int proc_qslice(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t begin = req[2].Int64();
	int64_t end = req[3].Int64();
	std::vector<std::string> list;
	int ret = serv->ssdb->lrange(req[1], begin, end, &list);


	if (ret < 0) {
		resp->reply_list(-1, list, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_list(ret, list);
	}

	return 0;
}

int proc_qget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t index = req[2].Int64();
	if (errno == EINVAL){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(INVALID_INT));
		return 0;
	}

	std::string val;
	int ret = serv->ssdb->LIndex(req[1], index, &val);
	if (ret < 0) {
		resp->reply_get(-1, &val, GetErrorInfo(ret).c_str());
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}

int proc_qset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &name = req[1];
	int64_t index = req[2].Int64();
	if (errno == EINVAL){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(INVALID_INT));
		return 0;
	}

	const Bytes &item = req[3];
	int ret = serv->ssdb->LSet(name, index, item);
	if(ret < 0){
		resp->push_back("error");
		resp->push_back(GetErrorInfo(ret));
	} else if ( ret == 0) {
		//???
		resp->push_back("error");
		resp->push_back("index out of range");
	} else{
		resp->push_back("ok");
	}
	return 0;
}
