/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "resp.h"
#include <stdio.h>
#include <sstream>
#include <iomanip>
#include <util/strings.h>

int Response::size() const{
	return (int)resp.size();
}

void Response::push_back(const std::string &s){
	resp.push_back(s);
}

void Response::add(int s){
	add((int64_t)s);
}

void Response::add(int64_t s){
	char buf[22] = {0};
	sprintf(buf, "%" PRId64 "", s);
	resp.push_back(buf);
}

void Response::add(uint64_t s){
	char buf[22] = {0};
	sprintf(buf, "%" PRIu64 "", s);
	resp.push_back(buf);
}

void Response::add(double s){
//	char buf[30];
//	snprintf(buf, sizeof(buf), "%f", s);
//
	std::ostringstream strs;
	strs << std::setprecision(64) << s;
	std::string str = strs.str();
//	std::string str = std::to_string(s);

	resp.push_back(str);
}

void Response::add(long double s){
	resp.push_back(str(s));
}

void Response::add(const std::string &s){
	resp.push_back(s);
}


void Response::reply_ok() {
	resp.push_back("ok");
}


void Response::reply_errror(const std::string &errmsg) {
	resp.push_back("error");
	resp.push_back(errmsg);
}


void Response::reply_status(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else{
		resp.push_back("ok");
	}
}

void Response::reply_bool(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else if(status == 0){
		resp.push_back("ok");
		resp.push_back("0");
	}else{
		resp.push_back("ok");
		resp.push_back("1");
	}
}

void Response::reply_int(int status, int64_t val, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}

void Response::reply_long_double(int status, long double val){
	if(status == -1){
		resp.push_back("error");
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}

void Response::reply_double(int status, double val){
	if(status == -1){
		resp.push_back("error");
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}

void Response::reply_get(int status, const std::string *val, const char *errmsg){
	if(status < 0){
		resp.push_back("error");
	}else if(status == 0){
		resp.push_back("not_found");
	}else{
		resp.push_back("ok");
		if(val){
			resp.push_back(*val);
		}
		return;
	}
	if(errmsg){
		resp.push_back(errmsg);
	}
}

void Response::mark_check() {
	checkKey = true;
}

void Response::reply_scan_ready() {
	resp.clear();
	resp.push_back("ok");
	resp.push_back("0");
}

void Response::reply_list_ready() {
	resp.push_back("ok");
}
