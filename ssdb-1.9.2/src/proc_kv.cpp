/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* kv */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"


int proc_type(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->type(req[1], &val);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_get(ret, &val);

	return 0;
}

int proc_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->get(req[1], &val);
	if(ret < 0){
		reply_err_return(ret);
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}

int proc_getset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	std::pair<std::string, bool> val;
	int ret = serv->ssdb->getset(req[1], val, req[2]);
	if(ret < 0){
		reply_err_return(ret);
	} else {
		if (val.second){
			resp->reply_get(1, &val.first);
		} else {
			resp->reply_get(0, &val.first);
		}
	}

	return 0;
}

int proc_append(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	uint64_t newlen = 0;
	int ret = serv->ssdb->append(req[1], req[2], &newlen);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_int(1, newlen);
	}
	return 0;
}

int proc_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	long long when = 0;

	TimeUnit tu = TimeUnit::Second;
	int flags = OBJ_SET_NO_FLAGS;
	if (req.size() > 3) {
		for (int i = 3; i < req.size(); ++i) {
			std::string key = req[i].String();
			strtolower(&key);

			if (key=="nx") {
				flags |= OBJ_SET_NX;
			} else if (key=="xx") {
				flags |= OBJ_SET_XX;
			} else if (key=="ex") {
				flags |= OBJ_SET_EX;
				tu = TimeUnit::Second;
			} else if (key=="px") {
				flags |= OBJ_SET_PX;
				tu = TimeUnit::Millisecond;
			}

			if (key=="nx" || key=="xx") {
				//nothing
			} else if (key=="ex" || key=="px") {
				i++;
				if (i >= req.size()) {
					reply_err_return(SYNTAX_ERR);

				} else {
					when = req[i].Int64();
					if (errno == EINVAL){
						reply_err_return(INVALID_INT);
					}

					if (when <= 0) {
						reply_err_return(INVALID_EX_TIME);
					}

				}
			} else {
				reply_err_return(SYNTAX_ERR);

			}
		}
	}

	if (when > 0) {

		Locking<Mutex> l(&serv->ssdb->expiration->mutex);

		int added = 0;
		int ret = serv->ssdb->set(req[1], req[2], flags, &added);
		if(ret < 0){
			reply_err_return(ret);
		} else if (added == 0) {
			resp->push_back("ok");
			resp->push_back("0");
			return 0;
		}


		ret = serv->ssdb->expiration->expire(req[1], (int64_t)when, tu);
		if(ret < 0){
			serv->ssdb->del(req[1]);
			reply_err_return(ret);
		} else {
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
		}


	} else {
		int added = 0;
		int ret = serv->ssdb->set(req[1], req[2], flags, &added);
		if(ret < 0){
			reply_err_return(ret);
		}

		resp->reply_bool(added);

	}

	return 0;
}

int proc_setnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int added = 0;
	int ret = serv->ssdb->set(req[1], req[2], OBJ_SET_NX, &added);
	if(ret < 0){
		reply_err_return(ret);
	} else {
		resp->reply_bool(added);
	}

	return 0;
}

int proc_setx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

    long long when;
    if (string2ll(req[3].data(), (size_t)req[3].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }
	if (when <= 0) {
		reply_err_return(INVALID_EX_TIME);
	}

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);

	int added = 0;
	int ret = serv->ssdb->set(req[1], req[2], OBJ_SET_NO_FLAGS, &added);
	if(ret < 0){
		reply_err_return(ret);
	}
	ret = serv->ssdb->expiration->expire(req[1], (int64_t)when, TimeUnit::Second);
	if(ret < 0){
        serv->ssdb->del(req[1]);
		reply_err_return(ret);
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_psetx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

    long long when;
    if (string2ll(req[3].data(), (size_t)req[3].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }
	if (when <= 0) {
		reply_err_return(INVALID_EX_TIME);
	}

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	int added = 0;
	int ret = serv->ssdb->set(req[1], req[2], OBJ_SET_NO_FLAGS, &added);
	if(ret < 0){
		reply_err_return(ret);
	}

	ret = serv->ssdb->expiration->expire(req[1], (int64_t)when, TimeUnit::Millisecond);
	if(ret < 0){
        serv->ssdb->del(req[1]);
		reply_err_return(ret);
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_pttl(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	//todo update
	int64_t ttl = serv->ssdb->expiration->pttl(req[1], TimeUnit::Millisecond);
	resp->push_back("ok");
	resp->push_back(str(ttl));
	return 0;
}

int proc_ttl(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	//todo update
	int64_t ttl = serv->ssdb->expiration->pttl(req[1], TimeUnit::Second);
	resp->push_back("ok");
	resp->push_back(str(ttl));
	return 0;
}

int proc_pexpire(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(3);

    long long when;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }

    Locking<Mutex> l(&serv->ssdb->expiration->mutex);
    std::string val;
    int ret = serv->ssdb->expiration->expire(req[1], (int64_t)when, TimeUnit::Millisecond);
	if (ret < 0) {
		reply_err_return(ret);
	}

	resp->reply_bool(ret);

    return 0;
}

int proc_expire(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

    long long when;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	std::string val;
	int ret = serv->ssdb->expiration->expire(req[1], (int64_t)when, TimeUnit::Second);
	if (ret < 0) {
		reply_err_return(ret);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_expireat(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

    long long ts_ms;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &ts_ms) == 0) {
		reply_err_return(INVALID_INT);
    }

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	std::string val;
	int ret = serv->ssdb->expiration->expireAt(req[1], (int64_t)ts_ms * 1000);
	if (ret < 0) {
		reply_err_return(ret);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_persist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	std::string val;
	int ret = serv->ssdb->expiration->persist(req[1]);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_bool(ret);
	return 0;
}

int proc_pexpireat(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

    long long ts_ms;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &ts_ms) == 0) {
		reply_err_return(INVALID_INT);
    }

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	std::string val;
	int ret = serv->ssdb->expiration->expireAt(req[1], (int64_t)ts_ms);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

    int count = 0;
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes key = *it;
		int ret = serv->ssdb->exists(key);
		if(ret == 1){
			count++;
		}
	}
    resp->reply_int(1, count);
	return 0;
}

int proc_multi_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 3 || req.size() % 2 != 1){
		reply_errinfo_return("ERR wrong number of arguments for MSET");
	}else{
		int ret = serv->ssdb->multi_set(req, 1);
		if(ret < 0){
			reply_err_return(ret);
		} else {
			resp->reply_int(ret, ret);
		}
	}
	return 0;
}

int proc_multi_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	int ret = serv->ssdb->multi_del(req, 1);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		resp->reply_int(0, ret);
	}
	return 0;
}

int proc_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(int i=1; i<req.size(); i++){
		std::string val;
		int ret = serv->ssdb->get(req[i], &val);
		if(ret == 1){
			resp->push_back(req[i].String());
			resp->push_back(val);
		}
	}
	return 0;
}

int proc_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	Locking<Mutex> l(&serv->ssdb->expiration->mutex);
	int ret = serv->ssdb->del(req[1]);
	if(ret < 0){
		reply_err_return(ret);
	} else if(ret == 0){
		resp->push_back("not_found");
		resp->push_back("0");
	} else{
//		serv->ssdb->expiration->persist(req[1]);

		resp->push_back("ok");
		resp->push_back(str(ret));
	}
	return 0;
}


int proc_scan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

    int cursorIndex = 1;

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

    serv->ssdb->scan(cursor, pattern, limit, resp->resp);

	return 0;
}

int proc_ssdb_dbsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t count = 0;

	std::string start;
	start.append(1, DataType::META);
	auto ssdb_it = std::unique_ptr<Iterator>(serv->ssdb->iterator(start, "", -1));
	while(ssdb_it->next()){
		Bytes ks = ssdb_it->key();

		if (ks.data()[0] != DataType::META) {
			break;
		}

		count++;
	}

    resp->reply_int(0, count);
	return 0;
}

int proc_ssdb_scan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	uint64_t limit = 1000;

	std::string pattern = "*";
	bool need_value = false;
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
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
		} else if (key=="value") {
			std::string has_value_s = (*(it+1)).String();
			strtolower(&has_value_s);

			need_value = (has_value_s=="on");
		} else {
			reply_err_return(SYNTAX_ERR);
		}
	}
	bool fulliter = (pattern == "*");

	auto ssdb_it = std::unique_ptr<Iterator>(serv->ssdb->iterator("", "", -1));
	resp->push_back("ok");
	while(ssdb_it->next()){

		if (fulliter || stringmatchlen(pattern.data(), pattern.length(), ssdb_it->key().data(), ssdb_it->key().size(), 0)) {
			resp->push_back(hexmem(ssdb_it->key().data(),ssdb_it->key().size()));
			if (need_value) {
				resp->push_back(hexmem(ssdb_it->val().data(),ssdb_it->val().size()));
			}

			limit --;
			if (limit == 0) {
				break; //stop now
			}

		} else {
			//skip
		}

	}

	return 0;
}

int proc_keys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;


	std::string pattern = "*";
	if (req.size() > 1) {
		pattern = req[1].String();
	}

	bool fulliter = (pattern == "*");

	std::string start;
	start.append(1, DataType::META);

	auto mit = std::unique_ptr<MIterator>(new MIterator(serv->ssdb->iterator(start, "", -1)));
    resp->push_back("ok");
    while(mit->next()){
		if (fulliter || stringmatchlen(pattern.data(), pattern.length(), mit->key.data(), mit->key.length(), 0)) {
			resp->push_back(mit->key);
		} else {
			//skip
		}

    }

	return 0;
}

// dir := +1|-1
static int _incr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(2);
	int64_t by = 1;
	if(req.size() > 2){
		by = req[2].Int64();
		if (errno == EINVAL){
			reply_err_return(INVALID_INT);
		}
	}
	int64_t new_val;
	int ret = ssdb->incr(req[1], dir * by, &new_val);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_incrbyfloat(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_NUM_PARAMS(3);
    long double by = req[2].LDouble();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	long double new_val = 0.0L;

	int ret = serv->ssdb->incrbyfloat(req[1], by, &new_val);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_long_double(ret, new_val);
	}

	return 0;
}

int proc_incr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, 1);
}

int proc_decr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, -1);
}

int proc_getbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	long long offset;
	string2ll(req[2].data(), (size_t)req[2].size(), &offset);
    if(offset < 0 || ((uint64_t)offset >> 3) >= Link::MAX_PACKET_SIZE * 4){
        std::string msg = "offset is is not an integer or out of range";
		reply_errinfo_return(msg);
    }

	int res = 0;
	int ret = serv->ssdb->getbit(req[1], (int64_t)offset, &res);
	if(ret < 0) {
		reply_err_return(ret);
	} else {
		resp->reply_bool(res);
	}

	return 0;
}

int proc_setbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &key = req[1];
	long long offset;
	string2ll(req[2].data(), (size_t)req[2].size(), &offset);

	int on = req[3].Int();
	if(on & ~1){
		reply_errinfo_return("bit is not an integer or out of range");
	}
	if(offset < 0 || ((uint64_t)offset >> 3) >= Link::MAX_PACKET_SIZE * 4){
		std::string msg = "offset is out of range [0, 4294967296)";
		reply_errinfo_return(msg);
	}

	int res = 0;
	int ret = serv->ssdb->setbit(key, (int64_t)offset, on, &res);
	if(ret < 0) {
		reply_err_return(ret);
	} else {
		resp->reply_bool(res);
	}

	return 0;
}

int proc_countbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		std::string str;
		int size = -1;
		if(req.size() > 3){
			size = req[3].Int();
			if (errno == EINVAL){
				reply_err_return(INVALID_INT);
			}

			str = substr(val, start, size);
		}else{
			str = substr(val, start, val.size());
		}
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}

int proc_bitcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
		if (errno == EINVAL){
			reply_err_return(INVALID_INT);
		}
	}
	int end = -1;
	if(req.size() > 3){
		end = req[3].Int();
		if (errno == EINVAL){
			reply_err_return(INVALID_INT);
		}
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		std::string str = str_slice(val, start, end);
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}


int proc_getrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &key = req[1];
	int64_t start = req[2].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}


	int64_t end = req[3].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	std::pair<std::string, bool> val;
	int ret = serv->ssdb->getrange(key, start, end, val);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		resp->reply_get(1, &val.first);
	}
	return 0;
}


int proc_substr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_getrange(net, link, req, resp);
}


int proc_setrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

 	int64_t start = req[2].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	if (start < 0) {
		reply_err_return(INDEX_OUT_OF_RANGE);
	}

	uint64_t new_len = 0;

	int ret = serv->ssdb->setrange(req[1], start, req[3], &new_len);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		resp->reply_int(1, new_len);
	}
	return 0;
}

int proc_strlen(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret < 0) {
		resp->reply_int(-1, val.size(), GetErrorInfo(ret).c_str());
	} else {
		resp->reply_int(ret, val.size());
	}
	return 0;
}
