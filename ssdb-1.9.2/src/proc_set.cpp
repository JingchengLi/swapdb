//
// Created by a1 on 16-11-22.
//
#include "serv.h"

int proc_sadd(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];
    std::string val;

    std::set<Bytes> mem_set;
    for (int j = 2; j < req.size(); ++j) {
        mem_set.insert(req[j]);
    }

    int64_t num = 0;

    int ret = serv->ssdb->sadd(name, mem_set, &num);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_srem(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];


    int64_t num = 0;

    int ret = serv->ssdb->srem(name, req, &num);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_scard(const Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    uint64_t len = 0;

    int ret = serv->ssdb->scard(req[1], &len);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, len);
    }

    return 0;
}


int proc_sismember(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    bool ismember = false;
    int ret = serv->ssdb->sismember(req[1], req[2], &ismember);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_bool(ret);
    } else {
        resp->reply_bool(ismember?1:0);
    }

    return 0;
}

int proc_smembers(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    resp->reply_list_ready();

    int ret = serv->ssdb->smembers(req[1],  resp->resp);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}


int proc_spop(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    int64_t pop_count = 1;
    if (req.size() >2) {
        pop_count = req[2].Int64();
        if (errno == EINVAL){
            reply_err_return(INVALID_INT);
        }
    }

    resp->reply_list_ready();

    int ret = serv->ssdb->spop(req[1], resp->resp, pop_count);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {

    }

    return 0;
}

int proc_srandmember(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    int64_t count = 1;
    if (req.size() >2) {
        count = req[2].Int64();
        if (errno == EINVAL){
            reply_err_return(INVALID_INT);
        }
    }

    resp->reply_list_ready();

    int ret = serv->ssdb->srandmember(req[1], resp->resp, count);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}


int proc_sscan(const Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    int cursorIndex = 2;

    Bytes cursor = req[cursorIndex];

    cursor.Uint64();
    if (errno == EINVAL){
        reply_err_return(INVALID_INT);
    }

    std::string pattern = "*";
    uint64_t limit = 10;

    std::vector<Bytes>::const_iterator it = req.begin() + cursorIndex + 1;
    for(; it != req.end(); it += 2){
        std::string key = (*it).String();
        strtolower(&key);

        if (key=="match") {
            pattern = (*(it+1)).String();
        } else if (key=="count") {
            limit =  (*(it+1)).Uint64();
            if (errno == EINVAL){
                reply_err_return(INVALID_INT);
            }
        } else {
            reply_err_return(SYNTAX_ERR);
        }
    }
    resp->reply_scan_ready();

    int ret =  serv->ssdb->sscan(req[1], cursor, pattern, limit, resp->resp);
    check_key(ret);
    if (ret < 0) {
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {
    }

    return 0;
}
