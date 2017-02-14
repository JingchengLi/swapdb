//
// Created by zts on 17-2-14.
//

#include "redis_stream.h"
#include "../util/log.h"

RedisUpstream::RedisUpstream(const std::string &ip, int port) : ip(ip), port(port) {
    client = getNewLink();
}

RedisUpstream::~RedisUpstream() {
    if (client != nullptr) {
        delete client;
    }
}

RedisClient *RedisUpstream::getNewLink() {
    return RedisClient::connect(ip.c_str(), port);
}

RedisClient *RedisUpstream::getLink() {
    if (client == nullptr) {
        client = getNewLink();
    }

    return client;
}

RedisClient *RedisUpstream::reset() {
    if (client != nullptr) {
        delete client;
    }
    client = getNewLink();
    return client;
}

RedisResponse *RedisUpstream::sendCommand(const std::vector<std::string> &args) {
    RedisResponse *res = nullptr;

    for (int i = 0; i < 5; ++i) {
        if (client == nullptr) {
            //reconnect
            reset();
            continue;
        }

        res = client->redisRequest(args);
        if (res == nullptr) {
            reset();
            continue;
        } else {
            break;
        }
    }

    return res;
}


