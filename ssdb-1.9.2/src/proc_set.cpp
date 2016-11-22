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

int proc_srem(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];
    const Bytes &key = req[2];
    int ret = serv->ssdb->srem(name, key);
    resp->reply_bool(ret);
    return 0;
}

int proc_scard(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(2);

    uint64_t len;
    int64_t ret = serv->ssdb->scard(req[1], &len);
    resp->reply_int(ret, len);
    return 0;
}
