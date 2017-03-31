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

class Response
{
public:
	std::vector<std::string> resp;

	bool checkKey = false;

	void mark_check();

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
	// the same as Redis.REPLY_BULK
	void reply_get(int status, const std::string *val=NULL, const char *errmsg=NULL);

};

#endif
