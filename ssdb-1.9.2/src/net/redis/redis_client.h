//
// Created by zts on 17-2-14.
//

#ifndef SSDB_LINK_REDIS_CLIENT_H
#define SSDB_LINK_REDIS_CLIENT_H


#include <net/link.h>
#include "reponse_redis.h"

class RedisClient {
public:
    RedisClient(Link *link);

    virtual ~RedisClient();

    RedisResponse *redisResponse();

    int redisRequestSend(const std::vector<std::string> &args);

    RedisResponse *redisRequest(const std::vector<std::string> &args);

    static RedisClient *connect(const char *host, int port, long timeout_ms = -1);

private:
    Link *so_link;


};


struct HostAndPort {
public:

    std::string ip;
    int port;

    HostAndPort(const std::string &ip, int port) : ip(ip), port(port) {}

    HostAndPort() {
        port = 0;
        ip = "";
    }
};


void appendRedisRequest(std::string &cmd, const std::vector<std::string> &args);


#endif //SSDB_LINK_REDIS_CLIENT_H
