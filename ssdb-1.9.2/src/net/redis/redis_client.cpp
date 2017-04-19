//
// Created by zts on 17-2-14.
//

#include "redis_client.h"


RedisResponse *RedisClient::redisResponse() {
    // Redis protocol supports
    // - + : $
    if (so_link->redis == NULL) {
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
                return NULL;
            }
        } else {
            so_link->input->decr(parsed);
            return resp;
        }
    }

    delete resp;
    return NULL;
}

int RedisClient::redisRequestSend(const std::vector<std::string> &args) {
    std::string tmp;
    tmp.append("*");
    tmp.append(str(args.size()));
    tmp.append("\r\n");
    for (const std::string &s: args) {
        tmp.append("$");
        tmp.append(str(s.size()));
        tmp.append("\r\n");
        tmp.append(s);
        tmp.append("\r\n");
    }
    tmp.append("\r\n");
    return so_link->output->append(tmp.data(), tmp.size());
}

RedisResponse *RedisClient::redisRequest(const std::vector<std::string> &args) {
    if (this->redisRequestSend(args) == -1) {
        return NULL;
    }

    if (so_link->flush() == -1) {
        return NULL;
    }

    return this->redisResponse();
}

RedisClient *RedisClient::connect(const char *host, int port) {
    Link *so_link = Link::connect(host, port);
    if (so_link == NULL) {
        return NULL;
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
