/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_REDISUPSTREAM_H
#define SSDB_REDISUPSTREAM_H


#include <net/redis/redis_client.h>

class RedisUpstream {
public:
    RedisUpstream(const std::string &ip, int port, long timeout_ms = -1);

    ~RedisUpstream();

    RedisClient *getNewLink();

    RedisClient *getLink();

    RedisClient *reset();

    RedisResponse *sendCommand(const std::vector<std::string> &args);

    void setMaxRetry(int maxRetry);

    bool isConnected() {
        return client != nullptr;
    }

private:
    RedisClient *client = nullptr;

    std::string ip;
    int port;
    long timeout_ms;
    int maxRetry = 5;
};

#endif //SSDB_REDISUPSTREAM_H
