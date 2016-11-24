//
// Created by a1 on 16-11-22.
//
#include "serv.h"

int proc_sadd(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];
//    const Bytes &key = req[2];
    std::string val;
//    int ret = serv->ssdb->sadd(name, key);
//    resp->reply_bool(ret);
    int64_t num = 0;
    int ret = serv->ssdb->multi_sadd(name, req, &num);
    resp->reply_int(ret, num);
    return 0;
}

int proc_srem(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];
//    const Bytes &key = req[2];
//    int ret = serv->ssdb->srem(name, key);
//    resp->reply_bool(ret);
    int64_t num = 0;
    int ret = serv->ssdb->multi_srem(name, req, &num);
    resp->reply_int(ret, num);
    return 0;
}

int proc_scard(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(2);

    uint64_t len;
    int ret = serv->ssdb->scard(req[1], &len);
    resp->reply_int(ret, len);
    return 0;
}

int proc_sdiff(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_sdiffstore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_sinter(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_sinterstore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_sismember(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;
    int ret = serv->ssdb->sismember(req[1], req[2]);
    resp->reply_bool(ret);
    return 0;
}

int proc_smembers(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;
    std::vector<std::string> members;
    int ret = serv->ssdb->smembers(req[1], members);
    if (ret == -1){
        resp->resp.push_back("error");
    } else if (ret == 0){
        resp->resp.push_back("not_found");
    } else{
        resp->resp.push_back("ok");
        std::vector<std::string>::iterator it = members.begin();
        for (;  it != members.end(); ++it) {
            resp->push_back(*it);
        }
    }

    return 0;
}

int proc_smove(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_spop(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_srandmember(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}

int proc_sunion(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;
    std::set<std::string> members;

    int ret = serv->ssdb->sunion(req, members);
    if (ret == -1){
        resp->resp.push_back("error");
    } else{
        std::set<std::string>::iterator it = members.begin();
        for (; it != members.end(); ++it) {
            resp->push_back(*it);
        }
    }

    return 0;
}

int proc_sunionstore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(3);

    int64_t len;
    int ret = serv->ssdb->sunionstore(req[1], req, &len);
    resp->reply_int(ret, len);
    return 0;
}

int proc_sscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}
