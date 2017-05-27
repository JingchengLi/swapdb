/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <util/cfree.h>
#include "version.h"
#include "util/log.h"
#include "util/strings.h"
#include "serv.h"
#include "redis/rdb.h"
#include "util/bytes.h"
#include "net/proc.h"
#include "net/server.h"
#include "replication.h"

extern "C" {
#include <redis/zmalloc.h>
#include <redis/lzf.h>
}

DEF_PROC(type);
DEF_PROC(get);
DEF_PROC(set);
DEF_PROC(append);
DEF_PROC(setx);
DEF_PROC(psetx);
DEF_PROC(setnx);
DEF_PROC(getset);
DEF_PROC(getbit);
DEF_PROC(setbit);
DEF_PROC(countbit);
DEF_PROC(substr);
DEF_PROC(getrange);
DEF_PROC(setrange);
DEF_PROC(strlen);
DEF_PROC(bitcount);
DEF_PROC(del);
DEF_PROC(incr);
DEF_PROC(incrbyfloat);
DEF_PROC(decr);
DEF_PROC(scan);
DEF_PROC(keys);
DEF_PROC(exists);
DEF_PROC(multi_get);
DEF_PROC(multi_set);
DEF_PROC(multi_del);
DEF_PROC(ttl);
DEF_PROC(pttl);
DEF_PROC(expire);
DEF_PROC(pexpire);
DEF_PROC(expireat);
DEF_PROC(pexpireat);
DEF_PROC(persist);
DEF_PROC(hsize);
DEF_PROC(hget);
DEF_PROC(hset);
DEF_PROC(hsetnx);
DEF_PROC(hdel);
DEF_PROC(hincr);
DEF_PROC(hincrbyfloat);
DEF_PROC(hgetall);
DEF_PROC(hscan);
DEF_PROC(hkeys);
DEF_PROC(hvals);
DEF_PROC(hexists);
DEF_PROC(hmget);
DEF_PROC(hmset);
DEF_PROC(sadd);
DEF_PROC(srem);
DEF_PROC(scard);
//DEF_PROC(sdiff);
//DEF_PROC(sdiffstore);
//DEF_PROC(sinter);
//DEF_PROC(sinterstore);
DEF_PROC(sismember);
DEF_PROC(smembers);
//DEF_PROC(smove);
DEF_PROC(spop);
DEF_PROC(srandmember);
//DEF_PROC(sunion);
//DEF_PROC(sunionstore);
DEF_PROC(sscan);
DEF_PROC(zrank);
DEF_PROC(zrrank);
DEF_PROC(zrange);
DEF_PROC(zrrange);
DEF_PROC(zrangebyscore);
DEF_PROC(zrevrangebyscore);
DEF_PROC(zsize);
DEF_PROC(zget);
DEF_PROC(zincr);
DEF_PROC(zdecr);
DEF_PROC(zscan);
DEF_PROC(zcount);
DEF_PROC(zremrangebyrank);
DEF_PROC(zremrangebyscore);
DEF_PROC(multi_zset);
DEF_PROC(multi_zdel);
DEF_PROC(zlexcount);
DEF_PROC(zrangebylex);
DEF_PROC(zremrangebylex);
DEF_PROC(zrevrangebylex);
DEF_PROC(qsize);
DEF_PROC(qpush_front);
DEF_PROC(qpush_frontx);
DEF_PROC(qpush_back);
DEF_PROC(qpush_backx);
DEF_PROC(qpop_front);
DEF_PROC(qpop_back);
DEF_PROC(qslice);
DEF_PROC(qtrim);
DEF_PROC(qget);
DEF_PROC(qset);

DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(dbsize);
DEF_PROC(compact);
DEF_PROC(flush);
DEF_PROC(flushdb);
DEF_PROC(dreply);
DEF_PROC(cursor_cleanup);
DEF_PROC(debug);
DEF_PROC(dump);
DEF_PROC(restore);
DEF_PROC(select);
DEF_PROC(client);
DEF_PROC(quit);
DEF_PROC(replic);
DEF_PROC(replic_info);
DEF_PROC(slowlog);

DEF_PROC(migrate);

DEF_PROC(ssdb_scan);
DEF_PROC(ssdb_dbsize);
DEF_PROC(ssdb_sync);

DEF_PROC(redis_req_dump);
DEF_PROC(redis_req_restore);
DEF_PROC(rr_do_flushall);
DEF_PROC(rr_flushall_check);
DEF_PROC(rr_check_write);
DEF_PROC(rr_make_snapshot);
DEF_PROC(rr_transfer_snapshot);
DEF_PROC(rr_del_snapshot);

DEF_PROC(repopid);


DEF_BPROC(COMMAND_DATA_SAVE);
DEF_BPROC(COMMAND_DATA_DUMP);

#define REG_PROC(c, f)     net->proc_map.set_proc(#c, f, proc_##c)

#define BPROC(c)  bproc_##c

void SSDBServer::reg_procs(NetworkServer *net) {
    REG_PROC(type, "rt");
    REG_PROC(get, "rt");
    REG_PROC(set, "wt");
    REG_PROC(append, "wt");
    REG_PROC(del, "wt");
    REG_PROC(setx, "wt");
    REG_PROC(psetx, "wt");
    REG_PROC(setnx, "wt");
    REG_PROC(getset, "wt");
    REG_PROC(getbit, "rt");
    REG_PROC(setbit, "wt");
    REG_PROC(countbit, "rt");
    REG_PROC(substr, "rt");
    REG_PROC(getrange, "rt");
    REG_PROC(setrange, "wt");
    REG_PROC(strlen, "rt");
    REG_PROC(bitcount, "rt");
    REG_PROC(incr, "wt");
    REG_PROC(incrbyfloat, "wt");
    REG_PROC(decr, "wt");
    REG_PROC(scan, "rt");
    REG_PROC(keys, "rt");
    REG_PROC(exists, "rt");
    REG_PROC(multi_get, "rt");
    REG_PROC(multi_set, "wt");
    REG_PROC(multi_del, "wt");
    REG_PROC(ttl, "rt");
    REG_PROC(pttl, "rt");
    REG_PROC(expire, "wt");
    REG_PROC(pexpire, "wt");
    REG_PROC(expireat, "wt");
    REG_PROC(pexpireat, "wt");
    REG_PROC(persist, "wt");

    REG_PROC(hsize, "rt");
    REG_PROC(hget, "rt");
    REG_PROC(hset, "wt");
    REG_PROC(hsetnx, "wt");
    REG_PROC(hincr, "wt");
    REG_PROC(hincrbyfloat, "wt");
    REG_PROC(hgetall, "rt");
    REG_PROC(hscan, "rt");
    REG_PROC(hkeys, "rt");
    REG_PROC(hvals, "rt");
    REG_PROC(hexists, "rt");
    REG_PROC(hmget, "rt");
    REG_PROC(hmset, "wt");
    REG_PROC(hdel, "wt");

    REG_PROC(sadd, "wt");
    REG_PROC(srem, "wt");
    REG_PROC(scard, "rt");
//    REG_PROC(sdiff, "rt");
//    REG_PROC(sdiffstore, "wt");
//    REG_PROC(sinter, "rt");
//    REG_PROC(sinterstore, "wt");
    REG_PROC(sismember, "rt");
    REG_PROC(smembers, "rt");
//    REG_PROC(smove, "wt");
    REG_PROC(spop, "wt");
    REG_PROC(srandmember, "rt");
//    REG_PROC(sunion, "rt");
//    REG_PROC(sunionstore, "wt");
    REG_PROC(sscan, "rt");

    // because zrank may be extremly slow, execute in a seperate thread
    REG_PROC(zrank, "rt");
    REG_PROC(zrrank, "rt");
    REG_PROC(zrange, "rt");
    REG_PROC(zrrange, "rt");
    REG_PROC(zrangebyscore, "rt");
    REG_PROC(zrevrangebyscore, "rt");
    REG_PROC(zsize, "rt");
    REG_PROC(zget, "rt");
    REG_PROC(zincr, "wt");
    REG_PROC(zdecr, "wt");
    REG_PROC(zscan, "rt");
    REG_PROC(zcount, "rt");
    REG_PROC(zremrangebyrank, "wt");
    REG_PROC(zremrangebyscore, "wt");
    REG_PROC(multi_zset, "wt");
    REG_PROC(multi_zdel, "wt");
    REG_PROC(zlexcount, "rt");
    REG_PROC(zrangebylex, "rt");
    REG_PROC(zremrangebylex, "wt");
    REG_PROC(zrevrangebylex, "rt");

    REG_PROC(qsize, "rt");
    REG_PROC(qpush_frontx, "wt");
    REG_PROC(qpush_front, "wt");
    REG_PROC(qpush_back, "wt");
    REG_PROC(qpush_backx, "wt");
    REG_PROC(qpop_front, "wt");
    REG_PROC(qpop_back, "wt");
    REG_PROC(qslice, "rt");
    REG_PROC(qget, "rt");
    REG_PROC(qset, "wt");
    REG_PROC(qtrim, "wt");

    REG_PROC(cursor_cleanup, "rt");
    REG_PROC(dump, "wt"); //auctual read but ...
    REG_PROC(restore, "wt");
    REG_PROC(redis_req_dump, "wt"); //auctual read but ...
    REG_PROC(redis_req_restore, "wt");

    REG_PROC(select, "rt");
    REG_PROC(migrate, "rt");
    REG_PROC(client, "r");
    REG_PROC(quit, "r");
    REG_PROC(replic, "b");
    REG_PROC(replic_info, "r");


    REG_PROC(dreply, "r");
    REG_PROC(flushdb, "wt");
    REG_PROC(flush, "wt");

    REG_PROC(slowlog, "r"); // attention!

    REG_PROC(info, "r");
    REG_PROC(version, "r");
    REG_PROC(dbsize, "rt");
    // doing compaction in a reader thread, because we have only one
    // writer thread(for performance reason); we don't want to block writes
    REG_PROC(compact, "rt");
    REG_PROC(debug, "wt");

    REG_PROC(ssdb_scan, "wt");
    REG_PROC(ssdb_dbsize, "wt");
    REG_PROC(ssdb_sync, "b");

    REG_PROC(rr_do_flushall, "wt");
    REG_PROC(rr_flushall_check, "wt");
    REG_PROC(rr_check_write, "wt");
    REG_PROC(rr_make_snapshot, "r");
    REG_PROC(rr_transfer_snapshot, "b");
    REG_PROC(rr_del_snapshot, "r");

    REG_PROC(repopid, "wt");
}

#define COMMAND_DATA_SAVE 1
#define COMMAND_DATA_DUMP 2


SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net) {
    this->ssdb = (SSDBImpl *) ssdb;
    this->meta = meta;

    net->data = this;
    this->reg_procs(net);

    {
        const Config *upstream_conf = conf.get("upstream");
        if (upstream_conf != NULL) {
            std::string ip = conf.get_str("upstream.ip");
            int port = conf.get_num("upstream.port");

            log_info("upstream: %s:%d", ip.c_str(), port);

            redisConf = HostAndPort(ip, port);
        }
    }
}

SSDBServer::~SSDBServer() {

    {
        Locking<Mutex> l(&replicMutex);
        if (replicSnapshot != nullptr) {
            ssdb->ReleaseSnapshot(replicSnapshot);
        }
    }

    log_info("SSDBServer finalized");
}

/*********************/


int proc_dreply(Context &ctx, Link *link, const Request &req, Response *resp) {
    //for QA only
    link->append_reply = true;
    resp->reply_ok();

    return 0;
}

int proc_flushdb(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

	log_warn("[!!!] do flushdb");
	serv->ssdb->flushdb(ctx);
	resp->reply_ok();

    return 0;
}
int proc_flush(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

	serv->ssdb->flush(ctx);
	resp->reply_ok();

    return 0;
}

int proc_select(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}


int proc_client(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}


int proc_slowlog(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(2);
    std::string action = req[1].String();
    strtolower(&action);

    if (action == "reset") {
        ctx.net->slowlog.reset();
        resp->reply_ok();

        {
            /*
             * raw redis reply
             */
            resp->redisResponse = new RedisResponse("OK");
            resp->redisResponse->type = REDIS_REPLY_STATUS;
        }
    } else if (action == "len") {
        uint64_t len = ctx.net->slowlog.len();
        resp->reply_int(1, len);

        {
            /*
             * raw redis reply
             */
            resp->redisResponse = new RedisResponse((long long int) ctx.net->slowlog.len());
        }
    } else if (action == "get") {
        resp->reply_list_ready();
        const auto &history = ctx.net->slowlog.history;

        for (int i = 0; i < history.size(); ++i) {
            const SlowlogEntry &h = history[i];
            resp->push_back(h.String());
        }

        {
            /*
             * raw redis reply
             */
            resp->redisResponse = new RedisResponse();
            resp->redisResponse->type = REDIS_REPLY_ARRAY;

            for (const auto &h : history) {
                auto item = new RedisResponse();
                item->type = REDIS_REPLY_ARRAY;
                item->push_back(new RedisResponse((long long int) h.id));
                item->push_back(new RedisResponse(h.ts));
                item->push_back(new RedisResponse(h.duration * 1000));
                item->push_back(new RedisResponse(h.req));

                resp->redisResponse->push_back(item);
            }
        }

    } else {
        reply_err_return(INVALID_ARGS);
    }

    return 0;
}


int proc_debug(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    std::string action = req[1].String();
    strtolower(&action);

    if (action == "segfault") {
        *((char *) -1) = 'x';
    } else if (action == "sleep") {
        CHECK_NUM_PARAMS(3);
        double dtime = req[2].Double();

        long long utime = dtime*1000000;
        struct timespec tv;

        tv.tv_sec = utime / 1000000;
        tv.tv_nsec = (utime % 1000000) * 1000;
        nanosleep(&tv, NULL);

    } else if (action == "digest") {

        std::string res;
        int ret = serv->ssdb->digest(&res);
        if (ret < 0) {
            reply_err_return(ret);
        }

        resp->reply_ok();
        resp->add(res);

        return 0;
    } else if (action == "populate") {
        CHECK_NUM_PARAMS(3);

        int64_t count = req[2].Int64();

        PTimer timer("DEBUG_POPULATE");
        timer.begin();

        for (long i = 0; i < count; ++i) {
            char kbuf[128] = {0};
            snprintf(kbuf, sizeof(kbuf), "%s:%lu", "key", i);

            char vbuf[128] = {0};
            snprintf(vbuf, sizeof(vbuf), "%s:%lu", "value", i);

            int added = 0;
            int ret = serv->ssdb->set(ctx, Bytes(kbuf), Bytes(vbuf), OBJ_SET_NO_FLAGS, 0, &added);
            if (ret < 0) {
                reply_err_return(ret);
            }

        }

        timer.end(str(count) + " keys");
    }

    resp->reply_ok();
    return 0;
}


int proc_quit(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}


int proc_restore(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(4);

    int64_t ttl = req[2].Int64();
    if (errno == EINVAL || ttl < 0) {
        reply_err_return(INVALID_EX_TIME);
    }

    bool replace = false;
    if (req.size() > 4) {
        std::string q4 = req[4].String();
        strtoupper(&q4);
        if (q4 == "REPLACE") {
            replace = true;
        } else {
            reply_err_return(SYNTAX_ERR);
        }
    }

    std::string val;

    PTST(restore, 0.01)
    int ret = serv->ssdb->restore(ctx, req[1], ttl, req[3], replace, &val);
    PTE(restore, req[1].String())

    if (ret < 0) {
        log_warn("%s, %s : %s", GetErrorInfo(ret).c_str(), hexmem(req[1].data(), req[1].size()).c_str(),
                 hexmem(req[3].data(), req[3].size()).c_str());
        reply_err_return(ret);
    } else {
        resp->reply_get(ret, &val);
    }

    return 0;
}

int proc_dump(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    std::string val;

    PTST(dump, 0.01)
    int ret = serv->ssdb->dump(ctx, req[1], &val);
    PTE(dump, req[1].String())

    check_key(ret);
    resp->reply_get(ret, &val);
    return 0;
}


int proc_cursor_cleanup(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    serv->ssdb->redisCursorCleanup();

    return 0;
}


int proc_redis_req_restore(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(4);

    int64_t ttl = req[2].Int64();
    if (errno == EINVAL || ttl < 0) {
        reply_err_return(INVALID_INT);
    }

    bool replace = false;


    if (req.size() > 4) {

        std::vector<Bytes>::const_iterator it = req.begin() + 4;
        for (; it != req.end(); it += 1) {
            std::string key = (*it).String();
            strtoupper(&key);

            if (key == "REPLACE") {
                replace = true;
            } else {
                reply_err_return(SYNTAX_ERR);
            }
        }


    }

    TransferJob *job = new TransferJob(ctx, COMMAND_DATA_SAVE, req[1].String(),
                                       new DumpData(req[1].String(), req[3].String(), ttl, replace));
    job->proc = BPROC(COMMAND_DATA_SAVE);

    ctx.net->redis->push(job);

    std::string val = "OK";
    resp->reply_get(1, &val);
    return 0;
}


int proc_redis_req_dump(Context &ctx, Link *link, const Request &req, Response *resp) {
    CHECK_NUM_PARAMS(2);

    TransferJob *job = new TransferJob(ctx, COMMAND_DATA_DUMP, req[1].String());
    job->proc = BPROC(COMMAND_DATA_DUMP);

    //TODO push1st
    ctx.net->redis->push(job);

    std::string val = "OK";
    resp->reply_get(1, &val);
    return 0;
}

int proc_compact(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    serv->ssdb->compact();
    resp->reply_ok();
    return 0;
}

int proc_dbsize(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    uint64_t size = serv->ssdb->size();
    resp->reply_int(1, size);

    return 0;
}

int proc_version(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back(SSDB_VERSION);
    return 0;
}

int proc_info(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    resp->push_back("ok");
    resp->push_back("ssdb-server");
    resp->push_back("version");
    resp->push_back(SSDB_VERSION);
    resp->push_back("engine");
    resp->push_back(SSDB_ENGINE);
    {
        resp->push_back("links");
        resp->add(ctx.net->link_count);
    }
    {
        int64_t calls = 0;
        proc_map_t::iterator it;
        for (it = ctx.net->proc_map.begin(); it != ctx.net->proc_map.end(); it++) {
            Command *cmd = it->second;
            calls += cmd->calls;
        }
        resp->push_back("total_calls");
        resp->add(calls);
    }

    {
        uint64_t size = serv->ssdb->size();
        resp->push_back("dbsize");
        resp->push_back(str(size));
    }

    // todo check
#ifdef USE_LEVELDB
    if(req.size() == 1 || req[1] == "leveldb"){
#else
    if (req.size() == 1 || req[1] == "leveldb" || req[1] == "rocksdb") {
#endif
        std::vector<std::string> tmp = serv->ssdb->info();
        for (int i = 0; i < (int) tmp.size(); i++) {
            std::string block = tmp[i];
            resp->push_back(block);
        }
    }

    if (req.size() > 1 && req[1] == "cmd") {
        proc_map_t::iterator it;
        for (it = ctx.net->proc_map.begin(); it != ctx.net->proc_map.end(); it++) {
            Command *cmd = it->second;
            resp->push_back("cmd." + cmd->name);
            char buf[128];
            snprintf(buf, sizeof(buf), "calls: %" PRIu64 "\ttime_wait: %.0f\ttime_proc: %.0f",
                     cmd->calls, cmd->time_wait, cmd->time_proc);
            resp->push_back(buf);
        }
    }

    return 0;
}


int proc_replic_info(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;


    const char* ReplicStateString[] = {
            "REPLIC_START",
            "REPLIC_TRANS",
            "REPLIC_END"
    };


    resp->reply_list_ready();
    {
        //update replic stats
        Locking<Mutex> l(&serv->replicMutex);

        resp->push_back("replicNumStarted");
        resp->add(serv->replicNumStarted);
        resp->push_back("replicNumFailed");
        resp->add(serv->replicNumFailed);
        resp->push_back("replicNumFinished");
        resp->add(serv->replicNumFinished);
        resp->push_back("replicState");
        resp->push_back(ReplicStateString[serv->replicState]);
    }

    return 0;
}


int proc_replic(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(3);
    std::string ip = req[1].String();
    int port = req[2].Int();

    {
        Locking<Mutex> l(&serv->replicMutex);
        serv->replicSnapshot = serv->ssdb->GetSnapshot();

        serv->replicState = REPLIC_TRANS;
        serv->replicNumStarted++;
    }

    ReplicationJob *job = new ReplicationJob(ctx, HostAndPort{ip, port}, link);

    ctx.net->replication->push(job);


    resp->resp.clear();
    return PROC_BACKEND;
}


int proc_rr_check_write(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back("rr_check_write ok");
    return 0;
}

int proc_rr_flushall_check(Context &ctx, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back("rr_flushall_check ok");
    return 0;
}


int proc_rr_do_flushall(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

	log_warn("[!!!] do flushall");
	std::queue<TransferJob *> discarded_jobs = ctx.net->redis->discard();

	log_warn("[!!!] discard %d TransferJob waiting for remain jobs", discarded_jobs.size());
	while (!discarded_jobs.empty())
	{
		delete discarded_jobs.front();
		discarded_jobs.pop();
	}

	Locking<RecordMutex<Mutex>> gl(&serv->transfer_mutex_record_);

	log_warn("[!!!] TransferJob clear done , starting flushdb");

    if (serv->ssdb->expiration != nullptr) {
        serv->ssdb->expiration->clear();
    }

    int ret = serv->ssdb->flushdb(ctx);
	if (ret < 0) {
		resp->push_back("ok");
		resp->push_back("rr_do_flushall nok");
	} else {
		resp->push_back("ok");
		resp->push_back("rr_do_flushall ok");
	}

    return 0;
}

int proc_rr_make_snapshot(Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    log_debug("1:link address:%lld", link);


    {
        Locking<Mutex> l(&serv->replicMutex);

        //TODO 判断snapshot是否已经无效.再release
        if (serv->replicSnapshot != nullptr) {
            serv->ssdb->ReleaseSnapshot(serv->replicSnapshot);
        }
        serv->replicSnapshot = serv->ssdb->GetSnapshot();

        serv->replicState = REPLIC_START;
        serv->replicNumStarted = 0;
        serv->replicNumFinished = 0;
        serv->replicNumFailed = 0;
    }

    resp->push_back("ok");
    resp->push_back("rr_make_snapshot ok");
    return 0;
}

int proc_rr_transfer_snapshot(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(3);

    std::string ip = req[1].String();
    int port = req[2].Int();

    {
        Locking<Mutex> l(&serv->replicMutex);
        serv->replicState = REPLIC_TRANS;
        serv->replicNumStarted++;
    }

    log_info("transfer_snapshot start %s:%d , link address:%lld", ip.c_str(), port, link);

    link->quick_send({"ok","rr_transfer_snapshot ok"});

    ReplicationJob *job = new ReplicationJob(ctx, HostAndPort{ip, port}, link, true);
    ctx.net->replication->push(job);

    resp->resp.clear(); //prevent send resp
    return PROC_BACKEND;
}

int proc_rr_del_snapshot(Context &ctx, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    {
        Locking<Mutex> l(&serv->replicMutex);

        if(serv->replicState == REPLIC_TRANS) {
            log_error("The replication is not finish");
            resp->push_back("ok");
            resp->push_back("rr_del_snapshot nok");
        }

        if (serv->replicSnapshot != nullptr){
            serv->ssdb->ReleaseSnapshot(serv->replicSnapshot);
            serv->replicSnapshot = nullptr;
        }
        log_debug("3:link address:%lld", link);

        serv->replicState = REPLIC_START;
        serv->replicNumStarted = 0;
        serv->replicNumFinished = 0;
        serv->replicNumFailed = 0;
    }

    resp->push_back("ok");
    resp->push_back("rr_del_snapshot ok");
    return 0;
}



int proc_repopid(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(2);

    std::string action = req[1].String();
    strtolower(&action);

    if (action == "get") {
        std::string repo_val;
        int rret = serv->ssdb->raw_get(ctx, encode_repo_key(), serv->ssdb->handles[1], &repo_val);
        if (rret < 0) {
            reply_err_return(rret);
        } else if (rret == 0) {
            resp->push_back("ok");
            resp->push_back("repopid 0 0");

            {
                resp->redisResponse = new RedisResponse("repopid 0 0");
            }

            return 0;
        }

        RepoKey repoKey;
        if (repoKey.DecodeRepoKey(repo_val) == -1) {
            reply_errinfo_return("ERR DecodeRepoKey error");
        }

        std::string result = "repopid " + str(repoKey.timestamp) + " " + str(repoKey.id);
        resp->push_back("ok");
        resp->push_back(result);

        {
            resp->redisResponse = new RedisResponse(result);
        }

    } else if (action == "set") {
        CHECK_NUM_PARAMS(4);

        uint64_t timestamp = req[2].Uint64();
        if (errno == EINVAL){
            reply_err_return(INVALID_INT);
        }

        uint64_t id = req[3].Uint64();
        if (errno == EINVAL){
            reply_err_return(INVALID_INT);
        }

        ctx.replLink = true;

        ctx.currentSeqCnx.timestamp = timestamp;
        ctx.currentSeqCnx.id = id;

        if (ctx.lastSeqCnx.timestamp == 0) {
            ctx.lastSeqCnx.timestamp = 1;
        }

        resp->reply_ok();

        {
            resp->redisResponse = new RedisResponse("repopid setok");
        }

    } else {
        reply_err_return(INVALID_ARGS);
    }


    return 0;
}

int proc_after_proc(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *)ctx.net->data;

    if (req[0] == "repopid") {
        return 0;
    }

    log_debug("proc_after_proc");

//    std::pair<uint64_t, uint64_t> commitedInfo = serv->ssdb->getCommitedInfo();

//    resp->r.timestamp = commitedInfo.first;
//    resp->r.id = commitedInfo.second;


    return 0;
}



int proc_ssdb_sync(Context &ctx, Link *link, const Request &req, Response *resp){

    log_info("ssdb_sync , link address:%lld", link);

    ReplicationJob *job = new ReplicationJob(ctx, HostAndPort{link->remote_ip, link->remote_port}, link, true);
//	net->replication->push(job);

    pthread_t tid;
    int err = pthread_create(&tid, NULL, &ssdb_sync, job);
    if (err != 0) {
        log_fatal("can't create thread: %s", strerror(err));
        exit(0);
    }

    resp->resp.clear(); //prevent send resp
    return PROC_BACKEND;
}


int proc_migrate(Context &ctx, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    CHECK_NUM_PARAMS(6);

    int copy, replace, j;
    std::string host;
    int port = 0;
    std::string key;
    long dbid = 0;
    long timeout = 0;

    int write_error = 0;

    std::vector<std::string> kv;


    /* Initialization */
    copy = 0;
    replace = 0;

    /* To support the KEYS option we need the following additional state. */
    int first_key = 3; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */


    if (req.size() > 4) {
        for (j = 6; j < req.size(); j++) {
            std::string q4 = req[j].String();
            strtolower(&q4);

            if (q4 == "copy") {
                copy = 1;
            } else if (q4 == "replace") {
                replace = 1;
            } else if (q4 == "keys") {
                if (!req[3].empty()) {
                    reply_errinfo_return("ERR When using MIGRATE KEYS option, the key argument"
                                    " must be set to the empty string");
                }
                first_key = j+1;
                num_keys = (int) (req.size() - j - 1);
                break;
            } else {
                reply_err_return(SYNTAX_ERR);
            }
        }
    }


    host = req[1].String();
    port = req[2].Int();
    if (errno == EINVAL || port < 1) {
        reply_err_return(INVALID_INT);
    }

    key = req[3].Int();

    dbid = req[4].Int();
    if (errno == EINVAL || dbid < 0) {
        reply_err_return(INVALID_INT);
    }


    timeout = req[5].Int();
    if (errno == EINVAL) {
        reply_err_return(INVALID_INT);
    }

    if (timeout <= 0) timeout = 1000;

    for (j = 0; j < num_keys; j++) {
        int res = serv->ssdb->check_meta_key(ctx, req[first_key+j]);
        if (res < 0) {
            reply_err_return(res);
        } else if (res == 0){
            //ingnore
        } else {
            kv.emplace_back(req[first_key+j].String());
        }
    }


    if (kv.empty()) {
        resp->reply_ok();
        {
            resp->redisResponse = new RedisResponse("NOKEY");
            resp->redisResponse->type = REDIS_REPLY_STATUS;
        }
    }


    Link *cs = Link::connect(host.c_str(), port, timeout);
    if (cs == nullptr) {
        if (errno == EHOSTUNREACH) {
            reply_errinfo_return("ERR Can't connect to target node: Name or service not known");
        } else {
            reply_errinfo_return("IOERR error or timeout connecting to the client");
        }
    }

    cs->sendtimeout(timeout);
    cs->readtimeout(timeout);

    //managed link
    RedisClient r(cs);
    std::string cmd;

    if (dbid != 0) {
        appendRedisRequest(cmd, {"SELECT", str((int)dbid)});
    }

    /* Create RESTORE payload and generate the protocol to call the command. */
    for (j = 0; j < kv.size(); j++) {
        std::string payload;
        int ret = serv->ssdb->dump(ctx, kv[j], &payload);
         if (ret < 0) {
            reply_err_return(ret);
        } else if (ret == 0) {
             continue;
         }

        int64_t pttl = serv->ssdb->expiration->pttl(ctx, kv[j], TimeUnit::Millisecond);
        if (pttl == -1) {
            pttl = 0;
        } else if (pttl == -2) {
            continue;
        }

        std::vector<std::string> cmd_item;
//        cmd_item.emplace_back("RESTORE");
        cmd_item.emplace_back("RESTORE-ASKING");
        cmd_item.emplace_back(kv[j]);
        cmd_item.emplace_back(str(pttl));
        cmd_item.emplace_back(payload);
        if (replace != 0) cmd_item.emplace_back("REPLACE");

        appendRedisRequest(cmd, cmd_item);
    }


    /* Transfer the query to the other node in 64K chunks. */
    errno = 0;
    {
        auto buf = cmd.data();
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = cmd.size() - pos) > 0) {
            towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
            cs->output->append(buf + pos, (int) towrite);
            nwritten = cs->write();
            if (nwritten <0 || cs->output->size()!=0) {
                if (errno != 0) {
                    log_debug("%s" , strerror(errno));
                }

                reply_errinfo_return("ERR write failed");
            }
            pos += nwritten;
        }
    }



    if (dbid != 0) {
        std::unique_ptr<RedisResponse> buf1(r.redisResponse());
        if (buf1) {
            if (buf1->type == REDIS_REPLY_ERROR) {
                reply_errinfo_return(("ERR Target instance replied with error: " + buf1->str));
            }
        } else {
            reply_errinfo_return("IOERR error or timeout to target instance");
        }
    }

    /* Read the RESTORE replies. */
    std::string error_info_from_target;
    int socket_error = 0;

    for (j = 0; j < kv.size(); j++) {
        std::unique_ptr<RedisResponse> buf2(r.redisResponse());
        if (!buf2) {
            socket_error = 1;
            break;
        }

        if (buf2->type == REDIS_REPLY_ERROR) {
            error_info_from_target.append(" " + buf2->str);

        } else {
            if (!copy) {
                /* No COPY option: remove the local key, signal the change. */
                serv->ssdb->del(ctx, kv[j]);
            }
        }

    }

    /* If we are here and a socket error happened, we don't want to retry.
       * Just signal the problem to the client, but only do it if we don't
       * already queued a different error reported by the destination server. */
    if (socket_error) {
        reply_errinfo_return("IOERR error or timeout to target instance");
    }

    if (!error_info_from_target.empty()) {
        reply_errinfo_return(("ERR Target instance replied with error: " + error_info_from_target));
    }

     /* Success! Update the last_dbid in migrateCachedSocket, so that we can
     * avoid SELECT the next time if the target DB is the same. Reply +OK. */
    resp->reply_ok();

    return 0;
}

