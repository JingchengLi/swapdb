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
    req.push_back("dump");
    req.push_back("a");

    res = link->redisRequest(req)->toString();
    dump(res.data(),res.size());




    delete link;

    return 0;

}