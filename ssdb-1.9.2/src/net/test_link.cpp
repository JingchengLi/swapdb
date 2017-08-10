/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <memory>
#include "link.h"
#include "redis/redis_client.h"

int main(int argc, char **argv) {


    Link *link = Link::connect("127.0.0.1", 6379);

    RedisClient redisClient(link);


    if (std::unique_ptr<RedisResponse> r = std::unique_ptr<RedisResponse>(
            redisClient.redisRequest({"set", "a", "a1"}))) {
        std::string res = r->toString();
        dump(res.data(), res.size());
    }

    if (std::unique_ptr<RedisResponse> r = std::unique_ptr<RedisResponse>(redisClient.redisRequest({"DEL", "a"}))) {
        std::string res = r->toString();
        dump(res.data(), res.size());
    }

    if (std::unique_ptr<RedisResponse> r = std::unique_ptr<RedisResponse>(redisClient.redisRequest({"dump", "a"}))) {
        std::string res = r->toString();
        dump(res.data(), res.size());
    }



//    req.clear();
//    req.push_back("DEL");
//    req.push_back("b");
//
//    res = redisClient.redisRequest(req)->toString();
//    dump(res.data(), res.size());
//
//
//    req.clear();
//    req.push_back("sadd");
//    req.push_back("b");
//    req.push_back("1");
//    req.push_back("2");
//    req.push_back("3");
//
//    res = redisClient.redisRequest(req)->toString();
//    dump(res.data(), res.size());
//
//
//    req.clear();
//    req.push_back("smembers");
//    req.push_back("b");
//
//    res = redisClient.redisRequest(req)->toString();
//    dump(res.data(), res.size());



    return 0;

}