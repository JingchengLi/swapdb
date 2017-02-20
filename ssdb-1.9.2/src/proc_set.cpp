//
// Created by a1 on 16-11-22.
//
#include "serv.h"

int proc_sadd(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];
    std::string val;

    std::set<Bytes> mem_set;
    for (int j = 2; j < req.size(); ++j) {
        mem_set.insert(req[j]);
    }

    int64_t num = 0;

    int ret = serv->ssdb->sadd(name, mem_set, &num);

    if (ret < 0) {
        resp->reply_int(-1, 0, GetErrorInfo(ret).c_str());
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_srem(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    const Bytes &name = req[1];


    int64_t num = 0;

    int ret = serv->ssdb->srem(name, req, &num);

    if (ret < 0) {
        resp->reply_int(-1, 0, GetErrorInfo(ret).c_str());
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_scard(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(2);

    uint64_t len = 0;

    int ret = serv->ssdb->scard(req[1], &len);
    if (ret < 0) {
        resp->reply_int(-1, 0, GetErrorInfo(ret).c_str());
    } else {
        resp->reply_int(ret, len);
    }

    return 0;
}

//int proc_sdiff(NetworkServer *net, Link *link, const Request &req, Response *resp){
//    return 0;
//}
//
//int proc_sdiffstore(NetworkServer *net, Link *link, const Request &req, Response *resp){
//    return 0;
//}
//
//int proc_sinter(NetworkServer *net, Link *link, const Request &req, Response *resp){
//    return 0;
//}
//
//int proc_sinterstore(NetworkServer *net, Link *link, const Request &req, Response *resp){
//    return 0;
//}

int proc_sismember(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *)net->data;

    int ret = serv->ssdb->sismember(req[1], req[2]);

    if (ret < 0) {
        resp->reply_bool(-1, GetErrorInfo(ret).c_str());
    } else {
        resp->reply_bool(ret);
    }

    return 0;
}

int proc_smembers(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;

    std::vector<std::string> members;
    int ret = serv->ssdb->smembers(req[1], members);

    if (ret < 0){
        resp->resp.push_back("error");
         resp->resp.push_back(GetErrorInfo(ret));
    } else if (ret == 0){
        resp->resp.push_back("ok");
    } else{
        resp->resp.push_back("ok");
        std::vector<std::string>::const_iterator it = members.begin();
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
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;

    int64_t pop_count = 1;
    if (req.size() >2) {
        pop_count = req[2].Int64();
        if (errno == EINVAL){
            resp->push_back("error");
            resp->push_back(GetErrorInfo(INVALID_INT));
            return 0;
        }
    }
    std::vector<std::string> members;
    int ret = serv->ssdb->spop(req[1], members, pop_count);

    if (ret < 0){
        resp->resp.push_back("error");
        resp->resp.push_back(GetErrorInfo(ret));
    } else if (ret == 0){
        resp->resp.push_back("ok");
    } else{
        resp->resp.push_back("ok");
        std::vector<std::string>::const_iterator it = members.begin();
        for (;  it != members.end(); ++it) {
            resp->push_back(*it);
        }
    }

    return 0;
}

int proc_srandmember(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;

    int64_t count = 1;
    if (req.size() >2) {
        count = req[2].Int64();
        if (errno == EINVAL){
            resp->push_back("error");
            resp->push_back(GetErrorInfo(INVALID_INT));
            return 0;
        }
    }
    std::vector<std::string> members;
    int ret = serv->ssdb->srandmember(req[1], members, count);

    if (ret < 0){
        resp->resp.push_back("error");
        resp->resp.push_back(GetErrorInfo(ret));
    } else if (ret == 0){
        resp->resp.push_back("ok");
    } else{
        resp->resp.push_back("ok");
        std::vector<std::string>::const_iterator it = members.begin();
        for (;  it != members.end(); ++it) {
            resp->push_back(*it);
        }
    }

    return 0;
}

int proc_sunion(NetworkServer *net, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *)net->data;
    std::set<std::string> members;

    int ret = serv->ssdb->sunion(req, members);
    if (ret < 0){
        resp->resp.push_back("error");
        resp->resp.push_back(GetErrorInfo(ret));
    } else{
        resp->resp.push_back("ok");
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

    if (ret < 0) {
        resp->reply_int(-1, 0, GetErrorInfo(ret).c_str());
    } else {
        resp->reply_int(ret, len);
    }

    return 0;
}

int proc_sscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
    return 0;
}
