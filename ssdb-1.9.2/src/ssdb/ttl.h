/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_TTL_H_
#define SSDB_TTL_H_

#include "ssdb.h"
#include "../util/thread.h"
#include "../util/sorted_set.h"
#include <string>


enum TimeUnit{
	Second,
	Millisecond,
};

class ExpirationHandler
{
public:
	Mutex mutex;

	ExpirationHandler(SSDB *ssdb);
	~ExpirationHandler();


	int64_t pttl(const Bytes &key, TimeUnit tu);

	// The caller must hold mutex before calling set/del functions
	int persist(const Bytes &key);
	void delfastkey(const Bytes &key) { fast_keys.del(key.String()); }
	int expire(const Bytes &key, int64_t ttl, TimeUnit tu);
	int expireAt(const Bytes &key, int64_t ts_ms);

	void start();
	void stop();
	void clear() {
		fast_keys.clear();
	}

private:
	SSDB *ssdb;
	volatile bool thread_quit;
//	std::string list_name;
	int64_t first_timeout;
	SortedSet<int64_t> fast_keys;

	void expire_loop();
	static void* thread_func(void *arg);
	void load_expiration_keys_from_db(int num);
};


#endif
