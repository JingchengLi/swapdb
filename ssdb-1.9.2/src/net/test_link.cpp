//
// Created by zts on 17-1-3.
//

#include "link.h"
#include "redis/redis_client.h"

int main(int argc, char **argv) {


    Link *link = Link::connect("127.0.0.1", 6379);

    RedisClient redisClient(link);
    
    
    std::vector<std::string> req = {"DEL", "a"};

    std::string res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


    req = {"set", "a", "a1"};
    res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


    req = {"dump", "a"};
    res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


    req.clear();
    req.push_back("DEL");
    req.push_back("b");

    res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


    req.clear();
    req.push_back("sadd");
    req.push_back("b");
    req.push_back("1");
    req.push_back("2");
    req.push_back("3");

    res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


    req.clear();
    req.push_back("smembers");
    req.push_back("b");

    res = redisClient.redisRequest(req)->toString();
    dump(res.data(), res.size());


 
    return 0;

}