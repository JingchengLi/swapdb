/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#include "redis_client.h"


RedisResponse *RedisClient::redisResponse() {
    // Redis protocol supports
    // - + : $
    if (so_link->redis == nullptr) {
        so_link->redis = new RedisLink();
    }

    RedisResponse *resp = new RedisResponse();

    while (1) {
        int parsed = so_link->redis->recv_res(so_link->input, resp, 0);
        if (resp->status == REDIS_RESPONSE_ERR) {
            so_link->input->decr(parsed);
            return resp;
        } else if (resp->status == REDIS_RESPONSE_RETRY) { //retry
            resp->reset();
            if (so_link->read() <= 0) {
                delete resp;
                return nullptr;
            }
        } else {
            so_link->input->decr(parsed);
            return resp;
        }
    }

    delete resp;
    return nullptr;
}

int RedisClient::redisRequestSend(const std::vector<std::string> &args) {
    std::string tmp;
    tmp.append("*");
    tmp.append(str((int64_t)args.size()));
    tmp.append("\r\n");
    for (const std::string &s: args) {
        tmp.append("$");
        tmp.append(str((int64_t)s.size()));
        tmp.append("\r\n");
        tmp.append(s);
        tmp.append("\r\n");
    }
    tmp.append("\r\n");
    return so_link->output->append(tmp.data(), tmp.size());
}

RedisResponse *RedisClient::redisRequest(const std::vector<std::string> &args) {
    if (so_link == nullptr) {
        return nullptr;
    }

    if (this->redisRequestSend(args) == -1) {
        return nullptr;
    }

    if (so_link->flush() == -1) {
        return nullptr;
    }

    return this->redisResponse();
}

RedisClient *RedisClient::connect(const char *host, int port, long timeout_ms) {
    Link *so_link = Link::connect(host, port, timeout_ms);
    if (so_link == nullptr) {
        return nullptr;
    }

    RedisClient *redisClient = new RedisClient(so_link);
    return redisClient;
}

RedisClient::RedisClient(Link *link) : so_link(link) {
//    link->settimeout(1, 0);
}

RedisClient::~RedisClient() {
    delete so_link;
}



void appendRedisRequest(std::string &cmd, const std::vector<std::string> &args) {
    cmd.append("*");
    cmd.append(str((int64_t)args.size()));
    cmd.append("\r\n");
    for (const std::string &s: args) {
        cmd.append("$");
        cmd.append(str((int64_t)s.size()));
        cmd.append("\r\n");
        cmd.append(s);
        cmd.append("\r\n");
    }
    cmd.append("\r\n");
}
