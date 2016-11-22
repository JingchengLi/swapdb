//
// Created by a1 on 16-11-22.
//
#include "serv.h"

int proc_sadd(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];
    const Bytes &key = req[2];
    std::string val;
    int ret = serv->ssdb->sadd(name, key);
    resp->reply_bool(ret);
    return 0;
}
