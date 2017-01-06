//
// Created by zts on 17-1-3.
//

#include "link.h"

int main(int argc, char **argv) {


    Link* link = Link::connect("127.0.0.1" , 6379);

    std::vector<std::string> req;
    req.push_back("DEL");
    req.push_back("a");

    std::string res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());




    req.clear();
    req.push_back("set");
    req.push_back("a");
    req.push_back("a1");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());


    req.clear();
    req.push_back("dump");
    req.push_back("a");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());




    req.clear();
    req.push_back("DEL");
    req.push_back("b");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());


    req.clear();
    req.push_back("sadd");
    req.push_back("b");
    req.push_back("1");
    req.push_back("2");
    req.push_back("3");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());


    req.clear();
    req.push_back("smembers");
    req.push_back("b");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());




    delete link;

    return 0;

}