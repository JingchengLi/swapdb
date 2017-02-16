//
// Created by zts on 17-2-14.
//

#ifndef SSDB_REDISUPSTREAM_H
#define SSDB_REDISUPSTREAM_H


#include <net/redis/redis_client.h>

class RedisUpstream {
public:
    RedisUpstream(const std::string &ip, int port);

    ~RedisUpstream();

    RedisClient *getNewLink();

    RedisClient *getLink();

    RedisClient *reset();

    RedisResponse *sendCommand(const std::vector<std::string> &args);

private:
    RedisClient *client = nullptr;

    std::string ip;
    int port;
};

#endif //SSDB_REDISUPSTREAM_H
