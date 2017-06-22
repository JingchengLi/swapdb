/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_TTL_H_
#define SSDB_TTL_H_

#include "ssdb_impl.h"
#include "../util/thread.h"
#include "../util/sorted_set.h"
#include <string>

class SSDBImpl;

enum TimeUnit {
    Second,
    Millisecond,
};

class ExpirationHandler {
public:
    Mutex mutex;

    explicit ExpirationHandler(SSDBImpl *ssdb, bool ex_enable);

    ~ExpirationHandler();


    int64_t pttl(Context &ctx, const Bytes &key, TimeUnit tu);

    int convert2ms(int64_t *ttl, TimeUnit tu);

    // The caller must hold mutex before calling set/del functions
    int persist(Context &ctx, const Bytes &key);

    int expire(Context &ctx, const Bytes &key, int64_t ttl, TimeUnit tu);

    int expireAt(Context &ctx, const Bytes &key, int64_t pexpireat_ms) {
        leveldb::WriteBatch batch;
        return expireAt(ctx, key, pexpireat_ms, batch);
    }

    int expireAt(Context &ctx, const Bytes &key, int64_t pexpireat_ms, leveldb::WriteBatch &batch, bool lock = true);

    int contrlExpiration(bool if_enable);

    int start();

    int stop();

    int clear();


    int cancelExpiration(Context &ctx, const Bytes &key, leveldb::WriteBatch &batch);

private:
    SSDBImpl *ssdb;

    volatile bool expire_enable;

    volatile bool thread_quit;
    std::atomic<int64_t> first_timeout;
    SortedSet<int64_t> fast_keys;

    void _expire_loop();

    static void *_thread_func(void *arg);

    void _load_expiration_keys_from_db(int num);

    void _insertFastKey(const std::string &key, int64_t pexpireat_ms);
};


#endif
