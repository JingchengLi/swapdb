/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_RESP_H_
#define NET_RESP_H_

#include <unistd.h>
#include <inttypes.h>
#include <string>
#include <vector>
#include "redis/reponse_redis.h"



#define reply_err_return(n) resp->reply_errror(GetErrorInfo(n)); return 0

#define reply_errinfo_return(c) resp->reply_errror(c); return 0

#define check_key(ret) if ((ret) == 0) ctx.mark_check()


class Response
{
public:
	std::vector<std::string> resp;

	RedisResponse *redisResponse = nullptr;

	int size() const;
	void push_back(const std::string &s);
	void add(int s);
	void add(int64_t s);
	void add(uint64_t s);
	void add(double s);
	void add(long double s);
	void add(const std::string &s);

	void reply_errror(const std::string &errmsg);
	void reply_status(int status, const char *errmsg=NULL);
	void reply_bool(int status, const char *errmsg=NULL);
	void reply_int(int status, int64_t val, const char *errmsg=NULL);
	void reply_long_double(int status, long double val);
	void reply_double(int status, double val);
	void reply_not_found();
	void reply_scan_ready();
	void reply_list_ready();
	// the same as Redis.REPLY_BULK
	void reply_get(int status, const std::string *val=NULL, const char *errmsg=NULL);

	void reply_ok();
};

#endif
