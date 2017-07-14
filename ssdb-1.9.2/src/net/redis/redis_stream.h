//
// Created by zts on 17-2-14.
//

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
    int retryConnect = 5;
};

#endif //SSDB_REDISUPSTREAM_H
