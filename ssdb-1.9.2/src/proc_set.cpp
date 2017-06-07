//
// Created by a1 on 16-11-22.
//
#include "serv.h"

int proc_sadd(Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];
    std::string val;
    std::set<Bytes> mem_set;

    for_each(req.begin() + 2, req.end(), [&](Bytes b) {
        mem_set.insert(b);
    });

    int64_t num = 0;

    int ret = serv->ssdb->sadd(ctx, name, mem_set, &num);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_srem(Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];


    int64_t num = 0;

    int ret = serv->ssdb->srem(ctx, name, req, &num);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, num);
    }

    return 0;
}

int proc_scard(Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    uint64_t len = 0;

    int ret = serv->ssdb->scard(ctx, req[1], &len);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, len);
    }

    return 0;
}


int proc_sismember(Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    bool ismember = false;
    int ret = serv->ssdb->sismember(ctx, req[1], req[2], &ismember);
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

int proc_smembers(Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    resp->reply_list_ready();

    int ret = serv->ssdb->smembers(ctx, req[1],  resp->resp);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}


int proc_spop(Context &ctx, Link *link, const Request &req, Response *resp){
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

    int ret = serv->ssdb->spop(ctx, req[1], resp->resp, pop_count);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {

    }

    return 0;
}

int proc_srandmember(Context &ctx, Link *link, const Request &req, Response *resp){
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

    int ret = serv->ssdb->srandmember(ctx, req[1], resp->resp, count);
    check_key(ret);
    if (ret < 0){
        resp->resp.clear();
        reply_err_return(ret);
    }

    return 0;
}


int proc_sscan(Context &ctx, Link *link, const Request &req, Response *resp){
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    int cursorIndex = 2;

    Bytes cursor = req[cursorIndex];

    cursor.Uint64();
    if (errno == EINVAL){
        reply_err_return(INVALID_INT);
    }

    ScanParams scanParams;

    int ret = prepareForScanParams(req, cursorIndex + 1, scanParams);
    if (ret < 0) {
        reply_err_return(ret);
    }

    resp->reply_scan_ready();
    ret =  serv->ssdb->sscan(ctx, req[1], cursor, scanParams.pattern, scanParams.limit, resp->resp);
    check_key(ret);
    if (ret < 0) {
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {
    }

    return 0;
}
