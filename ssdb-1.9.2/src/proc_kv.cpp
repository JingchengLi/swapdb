/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* kv */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"


int proc_type(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->type(ctx, req[1], &val);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_get(1, &val);

	return 0;
}

int proc_get(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->get(ctx, req[1], &val);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	} else {
		resp->reply_get(ret, &val);
	}

	return 0;
}

int proc_getset(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	std::pair<std::string, bool> val;
	int ret = serv->ssdb->getset(ctx, req[1], val, req[2]);
	check_key(ret);
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

int proc_append(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	uint64_t newlen = 0;
	int ret = serv->ssdb->append(ctx, req[1], req[2], &newlen);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_int(1, newlen);
	}
	return 0;
}

int proc_set(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ttl = 0;

	TimeUnit tu = TimeUnit::Second;
	int flags = OBJ_SET_NO_FLAGS;
	if (req.size() > 3) {
		for (int i = 3; i < req.size(); ++i) {
			std::string key = req[i].String();
			strtolower(&key);

			if (key == "nx" && !(flags & OBJ_SET_XX)) {
				flags |= OBJ_SET_NX;
			} else if (key == "xx" && !(flags & OBJ_SET_NX)) {
				flags |= OBJ_SET_XX;
			} else if (key == "ex" && !(flags & OBJ_SET_PX)) {
				flags |= OBJ_SET_EX;
				tu = TimeUnit::Second;
			} else if (key == "px" && !(flags & OBJ_SET_EX)) {
				flags |= OBJ_SET_PX;
				tu = TimeUnit::Millisecond;
			} else {
				reply_err_return(SYNTAX_ERR);
			}

			if (key == "nx" || key == "xx") {
				//nothing
			} else if (key=="ex" || key=="px") {
				i++;
				if (i >= req.size()) {
					reply_err_return(SYNTAX_ERR);

				} else {
					ttl = req[i].Int64();
					if (errno == EINVAL){
						reply_err_return(INVALID_INT);
					}

					if (ttl <= 0) {
						reply_err_return(INVALID_EX_TIME);
					}

				}
			} else {
				reply_err_return(SYNTAX_ERR);

			}
		}
	}

	int t_ret = serv->ssdb->expiration->convert2ms(&ttl, tu);
	if (t_ret < 0) {
		reply_err_return(t_ret);
	}


	int added = 0;
	int ret = serv->ssdb->set(ctx, req[1], req[2], flags, ttl, &added);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_bool(added);


	return 0;
}

int proc_setnx(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

	int added = 0;
	int ret = serv->ssdb->set(ctx, req[1], req[2], OBJ_SET_NX, 0, &added);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	} else {
		resp->reply_bool(added);
	}

	return 0;
}

int proc_setx(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);


	int64_t ttl = req[3].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	if (ttl <= 0) {
		reply_err_return(INVALID_EX_TIME);
	}

	int t_ret = serv->ssdb->expiration->convert2ms(&ttl, TimeUnit::Second);
	if (t_ret < 0) {
		reply_err_return(t_ret);
	}

	int added = 0;
	int ret = serv->ssdb->set(ctx, req[1], req[2], OBJ_SET_EX, ttl, &added);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_bool(1);

	return 0;
}

int proc_psetx(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

    int64_t ttl = req[3].Int64();
    if (errno == EINVAL){
        reply_err_return(INVALID_INT);
    }

    if (ttl <= 0) {
        reply_err_return(INVALID_EX_TIME);
    }

    int t_ret = serv->ssdb->expiration->convert2ms(&ttl, TimeUnit::Millisecond);
    if (t_ret < 0) {
		reply_err_return(t_ret);
    }


	int added = 0;
	int ret = serv->ssdb->set(ctx, req[1], req[2], OBJ_SET_PX, ttl, &added);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

    resp->reply_bool(1);

    return 0;
}

int proc_pttl(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ttl = serv->ssdb->expiration->pttl(ctx, req[1], TimeUnit::Millisecond);
	if (ttl == -2) {
		check_key(0);
	}
	resp->reply_int(1, ttl);

	return 0;
}

int proc_ttl(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ttl = serv->ssdb->expiration->pttl(ctx, req[1], TimeUnit::Second);
	if (ttl == -2) {
		check_key(0);
	}
	resp->reply_int(1, ttl);

	return 0;
}

int proc_pexpire(Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(3);

    long long when;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }

    std::string val;
    int ret = serv->ssdb->expiration->expire(ctx, req[1], (int64_t)when, TimeUnit::Millisecond);
    if (ret < 0) {
		reply_err_return(ret);
	} else if (ret < 2) {
		check_key(0);
	}

	resp->reply_bool(ret);
    return 0;
}

int proc_expire(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

    long long when;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &when) == 0) {
		reply_err_return(INVALID_INT);
    }

	std::string val;
	int ret = serv->ssdb->expiration->expire(ctx, req[1], (int64_t)when, TimeUnit::Second);
    if (ret < 0) {
		reply_err_return(ret);
	} else if (ret < 2) {
		check_key(0);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_expireat(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

    long long ts_ms;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &ts_ms) == 0) {
		reply_err_return(INVALID_INT);
    }

	std::string val;
	int ret = serv->ssdb->expiration->expireAt(ctx, req[1], (int64_t)ts_ms * 1000);
    if (ret < 0) {
		reply_err_return(ret);
	} else if (ret < 2) {
		check_key(0);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_persist(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->expiration->persist(ctx, req[1]);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

	resp->reply_bool(ret);
	return 0;
}

int proc_pexpireat(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);

    long long ts_ms;
    if (string2ll(req[2].data(), (size_t)req[2].size(), &ts_ms) == 0) {
		reply_err_return(INVALID_INT);
    }

	std::string val;
	int ret = serv->ssdb->expiration->expireAt(ctx, req[1], (int64_t)ts_ms);
	if(ret < 0){
		reply_err_return(ret);
	} else if (ret < 2) {
		check_key(0);
	}

	resp->reply_bool(ret);

	return 0;
}

int proc_exists(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

    uint64_t count = 0;
	for_each(req.begin()+1 , req.end(), [&](Bytes key) {
		int ret = serv->ssdb->exists(ctx, key);
		if(ret == 1){
			count++;
		}
	});

    resp->reply_int(1, count);
	return 0;
}

int proc_multi_set(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	if(req.size() < 3 || req.size() % 2 != 1){
		reply_errinfo_return("ERR wrong number of arguments for MSET");
	}else{
		int ret = serv->ssdb->multi_set(ctx, req, 1);
		if(ret < 0){
			reply_err_return(ret);
		} else {
			resp->reply_int(ret, (uint64_t)ret);
		}
	}
	return 0;
}

int proc_multi_del(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	std::set<std::string> distinct_keys;

	for_each(req.begin() + 1, req.end(), [&](Bytes b) {
		distinct_keys.emplace(b.String());
	});

	int64_t num = 0;
	int ret = serv->ssdb->multi_del(ctx, distinct_keys, &num);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		check_key(0);
		resp->reply_int(0, num);
	}

	return 0;
}

int proc_multi_get(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	resp->reply_list_ready();
	for(int i=1; i<req.size(); i++){
		std::string val;
		int ret = serv->ssdb->get(ctx, req[i], &val);
//		if(ret < 0){
//			resp->resp.clear();
//			reply_err_return(ret);
//		}
		if(ret == 1){
			resp->push_back(req[i].String());
			resp->push_back(val);
		}
	}
	return 0;
}

int proc_del(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	int ret = serv->ssdb->del(ctx, req[1]);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	}

	std::string res = str(ret);
	resp->reply_get(ret, &res);

	return 0;
}


int proc_scan(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

    int cursorIndex = 1;

	Bytes cursor = req[cursorIndex];

    cursor.Uint64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	ScanParams scanParams;

	int ret = prepareForScanParams(req, cursorIndex + 1, scanParams);
	if (ret < 0) {
		reply_err_return(ret);
	}

    resp->reply_scan_ready();

    serv->ssdb->scan(cursor, scanParams.pattern, scanParams.limit, resp->resp);

	return 0;
}

int proc_ssdb_dbsize(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;

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

int proc_ssdb_scan(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
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
	resp->reply_list_ready();
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

int proc_keys(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;


	std::string pattern = "*";
	if (req.size() > 1) {
		pattern = req[1].String();
	}

	bool fulliter = (pattern == "*");

	std::string start;
	start.append(1, DataType::META);

	auto mit = std::unique_ptr<MIterator>(new MIterator(serv->ssdb->iterator(start, "", -1)));
	resp->reply_list_ready();
    while(mit->next()){
		if (fulliter || stringmatchlen(pattern.data(), pattern.size(), mit->key.data(), mit->key.size(), 0)) {
			resp->emplace_back(mit->key.String());
		} else {
			//skip
		}

    }

	return 0;
}

// dir := +1|-1
static int _incr(Context &ctx, SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(2);
	int64_t by = 1;
	if(req.size() > 2){
		by = req[2].Int64();
		if (errno == EINVAL){
			reply_err_return(INVALID_INT);
		}
	}
	int64_t new_val;
	int ret = ssdb->incr(ctx, req[1], dir * by, &new_val);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_incrbyfloat(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;

	CHECK_NUM_PARAMS(3);
    long double by = req[2].LDouble();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	long double new_val = 0.0L;

	int ret = serv->ssdb->incrbyfloat(ctx, req[1], by, &new_val);
	if(ret < 0){
		reply_err_return(ret);
	}else{
		resp->reply_long_double(ret, new_val);
	}

	return 0;
}

int proc_incr(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	return _incr(ctx, serv->ssdb, req, resp, 1);
}

int proc_decr(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	return _incr(ctx, serv->ssdb, req, resp, -1);
}

int proc_getbit(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(3);
	long long offset = 0;
	string2ll(req[2].data(), (size_t)req[2].size(), &offset);
    if(offset < 0 || ((uint64_t)offset >> 3) >= Link::MAX_PACKET_SIZE * 4){
        std::string msg = "ERR offset is is not an integer or out of range";
		reply_errinfo_return(msg);
    }

	int res = 0;
	int ret = serv->ssdb->getbit(ctx, req[1], (int64_t)offset, &res);
	check_key(ret);
	if(ret < 0) {
		reply_err_return(ret);
	} else {
		resp->reply_bool(res);
	}

	return 0;
}

int proc_setbit(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &name = req[1];
	long long offset = 0;
	string2ll(req[2].data(), (size_t)req[2].size(), &offset);

	int on = req[3].Int();
	if(on & ~1){
		reply_errinfo_return("ERR bit is not an integer or out of range");
	}
	if(offset < 0 || ((uint64_t)offset >> 3) >= Link::MAX_PACKET_SIZE * 4){
		reply_errinfo_return("ERR offset is out of range [0, 4294967296)");
	}

	int res = 0;
	int ret = serv->ssdb->setbit(ctx, name, (int64_t)offset, on, &res);
	check_key(ret);
	if(ret < 0) {
		reply_err_return(ret);
	} else {
		resp->reply_bool(res);
	}

	return 0;
}

int proc_countbit(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(ctx, key, &val);
	check_key(ret);
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
		resp->reply_int(0, (uint64_t)count);
	}
	return 0;
}

int proc_bitcount(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &name = req[1];
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
	int ret = serv->ssdb->get(ctx, name, &val);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		std::string str = str_slice(val, start, end);
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}


int proc_getrange(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &name = req[1];
	int64_t start = req[2].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}


	int64_t end = req[3].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	std::pair<std::string, bool> val;
	int ret = serv->ssdb->getrange(ctx, name, start, end, val);
	check_key(ret);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		resp->reply_get(1, &val.first);
	}
	return 0;
}


int proc_substr(Context &ctx, Link *link, const Request &req, Response *resp){
	return proc_getrange(ctx, link, req, resp);
}


int proc_setrange(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(4);

 	int64_t start = req[2].Int64();
	if (errno == EINVAL){
		reply_err_return(INVALID_INT);
	}

	if (start < 0) {
		reply_err_return(INDEX_OUT_OF_RANGE);
	}

	uint64_t new_len = 0;

	int ret = serv->ssdb->setrange(ctx, req[1], start, req[3], &new_len);
	if(ret < 0){
		reply_err_return(ret);
	} else{
		resp->reply_int(1, new_len);
	}
	return 0;
}

int proc_strlen(Context &ctx, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *) ctx.net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &name = req[1];
	std::string val;
	int ret = serv->ssdb->get(ctx, name, &val);
	check_key(ret);
	if(ret < 0) {
		reply_err_return(ret);
	} else {
		resp->reply_int(ret, val.size());
	}
	return 0;
}
