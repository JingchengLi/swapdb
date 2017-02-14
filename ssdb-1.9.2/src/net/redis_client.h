//
// Created by zts on 17-2-14.
//

#ifndef SSDB_LINK_REDIS_CLIENT_H
#define SSDB_LINK_REDIS_CLIENT_H


#include "link.h"

class RedisClient {
public:
    RedisClient(Link *link);

    virtual ~RedisClient();

    RedisResponse *redisResponse();

    int redisRequestSend(const std::vector<std::string> &args);

    RedisResponse *redisRequest(const std::vector<std::string> &args);

    static RedisClient *connect(const char *host, int port);

private:
    Link *so_link;


};


#endif //SSDB_LINK_REDIS_CLIENT_H
