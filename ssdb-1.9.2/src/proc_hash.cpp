/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* hash */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_hexists(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];
    const Bytes &key = req[2];
    std::pair<std::string, bool> val;
    int ret = serv->ssdb->hget(ctx, name, key, val);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_bool(0);
    } else {
        resp->reply_bool(val.second);
    }

    return 0;
}


int proc_hmset(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    if (req.size() < 4 || req.size() % 2 != 0) {
        reply_errinfo_return("wrong number of arguments for 'hmset' command");
    }

    std::map<Bytes, Bytes> kvs;
    const Bytes &name = req[1];
    std::vector<Bytes>::const_iterator it = req.begin() + 2;
    for (; it != req.end(); it += 2) {
        const Bytes &key = *it;
        const Bytes &val = *(it + 1);
        kvs[key] = val;
    }

    int ret = serv->ssdb->hmset(ctx, name, kvs);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }

    resp->reply_int(0, 1);

    return 0;
}

int proc_hdel(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const Bytes &name = req[1];

    std::set<Bytes> fields;
    for (int j = 2; j < req.size(); ++j) {
        fields.insert(req[j]);
    }

    int deleted = 0;
    int ret = serv->ssdb->hdel(ctx, name, fields, &deleted);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    }

    resp->reply_int(0, deleted);
    return 0;
}

int proc_hmget(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    Request::const_iterator it = req.begin() + 1;
    const Bytes name = *it;
    it++;

    std::vector<std::string> reqKeys;
    std::map<std::string, std::string> resMap;

    for (; it != req.end(); it += 1) {
        const Bytes &key = *it;
        reqKeys.push_back(key.String());
    }

    int ret = serv->ssdb->hmget(ctx, name, reqKeys, resMap);
    check_key(ret);
    if (ret == 1) {
        resp->reply_list_ready();

        for (const auto &reqKey : reqKeys) {

            auto pos = resMap.find(reqKey);
            if (pos == resMap.end()) {
                //
            } else {
                resp->push_back(pos->first);
                resp->push_back(pos->second);
            }

        }

    } else if (ret == 0) {
        resp->reply_list_ready();

        //nothing
    } else {
        reply_err_return(ret);
    }

    return 0;
}

int proc_hsize(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    uint64_t size = 0;
    int ret = serv->ssdb->hsize(ctx, req[1], &size);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, size);
    }

    return 0;
}

int proc_hset(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(4);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    int added = 0;
    int ret = serv->ssdb->hset(ctx, req[1], req[2], req[3], &added);
//    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_bool(ret);
    } else {
        resp->reply_bool(added);
    }

    return 0;
}

int proc_hsetnx(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(4);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    int added = 0;
    int ret = serv->ssdb->hsetnx(ctx, req[1], req[2], req[3], &added);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_bool(ret);
    } else {
        resp->reply_bool(added);
    }

    return 0;
}

int proc_hget(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    std::pair<std::string, bool> val;
    int ret = serv->ssdb->hget(ctx, req[1], req[2], val);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        if (val.second) {
            resp->reply_get(1, &val.first);
        } else {
            resp->reply_get(0, &val.first);
        }
    }

    return 0;
}


int proc_hgetall(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(2);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    std::map<std::string, std::string> resMap;
    int ret = serv->ssdb->hgetall(ctx, req[1], resMap);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_list_ready();

        //nothing
    } else {
        resp->reply_list_ready();

        for (const auto &res : resMap) {
            resp->push_back(res.first);
            resp->push_back(res.second);
        }

    }

    return 0;
}

int proc_hscan(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(3);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    int cursorIndex = 2;

    Bytes cursor = req[cursorIndex];

    cursor.Uint64();
    if (errno == EINVAL) {
        reply_err_return(INVALID_INT);
    }

    std::string pattern = "*";
    uint64_t limit = 10;

    std::vector<Bytes>::const_iterator it = req.begin() + cursorIndex + 1;
    for (; it != req.end(); it += 2) {
        std::string key = (*it).String();
        strtolower(&key);

        if (key == "match") {
            pattern = (*(it + 1)).String();
        } else if (key == "count") {
            limit = (*(it + 1)).Uint64();
            if (errno == EINVAL) {
                reply_err_return(INVALID_INT);
            }
        } else {
            reply_err_return(SYNTAX_ERR);
        }
    }
    resp->reply_scan_ready();

    int ret = serv->ssdb->hscan(ctx, req[1], cursor, pattern, limit, resp->resp);
    check_key(ret);
    if (ret < 0) {
        resp->resp.clear();
        reply_err_return(ret);
    } else if (ret == 0) {
    }

    return 0;
}


int proc_hkeys(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(5);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

//	uint64_t limit = req[4].Uint64();

    std::map<std::string, std::string> resMap;
    int ret = serv->ssdb->hgetall(ctx, req[1], resMap);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_list_ready();

        //nothing
    } else {
        resp->reply_list_ready();

        for (const auto &res : resMap) {
            //TODO 这里同时处理了kv 只是没有返回.
            resp->push_back(res.first);
//            resp->push_back(res.second);
        }
    }


    return 0;
}

int proc_hvals(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(5);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

//	uint64_t limit = req[4].Uint64();

    std::map<std::string, std::string> resMap;
    int ret = serv->ssdb->hgetall(ctx, req[1], resMap);
    check_key(ret);
    if (ret < 0) {
        reply_err_return(ret);
    } else if (ret == 0) {
        resp->reply_list_ready();

        //nothing
    } else {
        resp->reply_list_ready();

        for (const auto &res : resMap) {
            //TODO 这里同时处理了kv 只是没有返回.
//            resp->push_back(res.first);
            resp->push_back(res.second);
        }
    }

    return 0;
}

int proc_hincrbyfloat(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(4);

    long double by = req[3].LDouble();
    if (errno == EINVAL) {
        reply_err_return(INVALID_DBL);
    }

    long double new_val;
    int ret = serv->ssdb->hincrbyfloat(ctx, req[1], req[2], by, &new_val);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_long_double(ret, new_val);
    }
    return 0;

}

int proc_hincr(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(4);

    int64_t by = req[3].Int64();

    if (errno == EINVAL) {
        reply_err_return(INVALID_INT);
    }

    int64_t new_val = 0;
    int ret = serv->ssdb->hincr(ctx, req[1], req[2], by, &new_val);
    if (ret < 0) {
        reply_err_return(ret);
    } else {
        resp->reply_int(ret, new_val);
    }
    return 0;

}