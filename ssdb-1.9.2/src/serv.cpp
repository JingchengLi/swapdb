/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "version.h"
#include "util/log.h"
#include "util/strings.h"
#include "serv.h"
#include "redis/rdb.h"
#include "util/bytes.h"
#include "net/proc.h"
#include "net/server.h"

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
DEF_PROC(dump2);
DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(dbsize);
DEF_PROC(compact);
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
DEF_PROC(sync150);
DEF_PROC(slowlog);
DEF_PROC(ssdb_scan);
DEF_PROC(ssdb_dbsize);
DEF_PROC(redis_req_dump);
DEF_PROC(redis_req_restore);
DEF_PROC(rr_do_flushall);
DEF_PROC(rr_flushall_check);
DEF_PROC(rr_check_write);
DEF_PROC(rr_make_snapshot);
DEF_PROC(rr_transfer_snapshot);
DEF_PROC(rr_del_snapshot);
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
    REG_PROC(client, "r");
    REG_PROC(quit, "r");
    REG_PROC(replic, "b");
    REG_PROC(sync150, "r");


    REG_PROC(dreply, "r");
    REG_PROC(flushdb, "wt");

    REG_PROC(slowlog, "r"); // attention!

    REG_PROC(dump2, "b");
    REG_PROC(info, "r");
    REG_PROC(version, "r");
    REG_PROC(dbsize, "rt");
    // doing compaction in a reader thread, because we have only one
    // writer thread(for performance reason); we don't want to block writes
    REG_PROC(compact, "rt");
    REG_PROC(debug, "wt");

    REG_PROC(ssdb_scan, "wt");
    REG_PROC(ssdb_dbsize, "wt");

    REG_PROC(rr_do_flushall, "wt");
    REG_PROC(rr_flushall_check, "wt");
    REG_PROC(rr_check_write, "wt");
    REG_PROC(rr_make_snapshot, "r");
    REG_PROC(rr_transfer_snapshot, "b");
    REG_PROC(rr_del_snapshot, "r");
}

#define COMMAND_DATA_SAVE 1
#define COMMAND_DATA_DUMP 2


SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net) {
    this->ssdb = (SSDBImpl *) ssdb;
    this->meta = meta;

    net->data = this;
    this->reg_procs(net);

    backend_dump = new BackendDump(this->ssdb);

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
    delete backend_dump;

    {
        Locking<Mutex> l(&replicMutex);
        if (replicSnapshot != nullptr) {
            ssdb->ReleaseSnapshot(replicSnapshot);
        }
    }

    log_info("SSDBServer finalized");
}

/*********************/


int proc_dreply(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    //for QA only
    link->append_reply = true;
    resp->reply_ok();

    return 0;
}

int proc_flushdb(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;

	log_warn("[!!!] do flushdb");
	serv->ssdb->flushdb();
	resp->reply_ok();

    return 0;
}

int proc_select(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}


int proc_client(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}


int proc_slowlog(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(2);
    std::string action = req[1].String();
    strtolower(&action);

    if (action == "reset") {
        net->slowlog.reset();
        resp->reply_ok();

        {
            /*
             * raw redis reply
             */
            resp->redisResponse = new RedisResponse("OK");
            resp->redisResponse->type = REDIS_REPLY_STATUS;
        }
    } else if (action == "len") {
        uint64_t len = net->slowlog.len();
        resp->reply_int(1, len);

        {
            /*
             * raw redis reply
             */
            resp->redisResponse = new RedisResponse((long long int) net->slowlog.len());
        }
    } else if (action == "get") {
        resp->reply_list_ready();
        const auto &history = net->slowlog.history;

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


int proc_debug(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(2);

    std::string action = req[1].String();
    strtolower(&action);

    if (action == "segfault") {
        *((char *) -1) = 'x';
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
            int ret = serv->ssdb->set(Bytes(kbuf), Bytes(vbuf), OBJ_SET_NO_FLAGS, &added);
            if (ret < 0) {
                reply_err_return(ret);
            }

        }

        timer.end(str(count) + " keys");
    }

    resp->reply_ok();
    return 0;
}


int proc_quit(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->reply_ok();
    return 0;
}

int proc_dump2(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    serv->backend_dump->proc(link);
    return PROC_BACKEND;
}


int proc_restore(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
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
    int ret = serv->ssdb->restore(req[1], ttl, req[3], replace, &val);
    PTE(restore, req[1].String())

    if (ret > 0 && ttl > 0) {
        Locking<Mutex> l(&serv->ssdb->expiration->mutex);
        ret = serv->ssdb->expiration->expire(req[1], ttl, TimeUnit::Millisecond);
        if (ret < 0) {
            serv->ssdb->del(req[1]);
        }
    }

    if (ret < 0) {
        log_warn("%s, %s : %s", GetErrorInfo(ret).c_str(), hexmem(req[1].data(), req[1].size()).c_str(),
                 hexmem(req[3].data(), req[3].size()).c_str());
        reply_err_return(ret);
    } else {
        resp->reply_get(ret, &val);
    }

    return 0;
}

int proc_dump(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(2);

    std::string val;

    PTST(dump, 0.01)
    int ret = serv->ssdb->dump(req[1], &val);
    PTE(dump, req[1].String())

    check_key(ret);
    resp->reply_get(ret, &val);
    return 0;
}


int proc_cursor_cleanup(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(2);

    serv->ssdb->redisCursorCleanup();

    return 0;
}


int proc_redis_req_restore(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
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

    TransferJob *job = new TransferJob(serv, COMMAND_DATA_SAVE, req[1].String(),
                                       new DumpData(req[1].String(), req[3].String(), ttl, replace));
    job->proc = BPROC(COMMAND_DATA_SAVE);

    net->redis->push(job);

    std::string val = "OK";
    resp->reply_get(1, &val);
    return 0;
}


int proc_redis_req_dump(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(2);

    TransferJob *job = new TransferJob(serv, COMMAND_DATA_DUMP, req[1].String());
    job->proc = BPROC(COMMAND_DATA_DUMP);

    //TODO push1st
    net->redis->push(job);

    std::string val = "OK";
    resp->reply_get(1, &val);
    return 0;
}

int proc_compact(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    serv->ssdb->compact();
    resp->reply_ok();
    return 0;
}

int proc_dbsize(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    uint64_t size = serv->ssdb->size();
    resp->reply_int(1, size);

    return 0;
}

int proc_version(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back(SSDB_VERSION);
    return 0;
}

int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    resp->push_back("ok");
    resp->push_back("ssdb-server");
    resp->push_back("version");
    resp->push_back(SSDB_VERSION);
    resp->push_back("engine");
    resp->push_back(SSDB_ENGINE);
    {
        resp->push_back("links");
        resp->add(net->link_count);
    }
    {
        int64_t calls = 0;
        proc_map_t::iterator it;
        for (it = net->proc_map.begin(); it != net->proc_map.end(); it++) {
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
        for (it = net->proc_map.begin(); it != net->proc_map.end(); it++) {
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


int proc_replic(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    CHECK_NUM_PARAMS(3);
    std::string ip = req[1].String();
    int port = req[2].Int();

    {
        Locking<Mutex> l(&serv->replicMutex);
        serv->replicSnapshot = serv->ssdb->GetSnapshot();
    }

    ReplicationJob *job = new ReplicationJob(serv, HostAndPort{ip, port}, link);

    net->replication->push(job);


    resp->push_back("ok");
    return PROC_BACKEND;
}

static int ssdb_load_len(const char *data, int *offset, uint64_t *lenptr) {
    unsigned char buf[2];
    buf[0] = (unsigned char) data[0];
    buf[1] = (unsigned char) data[1];
    int type;
    type = (buf[0] & 0xC0) >> 6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        *lenptr = buf[0] & 0x3F;
        *offset = 1;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0] & 0x3F;
        *offset = 1;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        *lenptr = ((buf[0] & 0x3F) << 8) | buf[1];
        *offset = 2;
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        len = *(uint32_t *) (data + 1);
        *lenptr = be32toh(len);
        *offset = 1 + sizeof(uint32_t);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        len = *(uint64_t *) (data + 1);
        *lenptr = be64toh(len);
        *offset = 1 + sizeof(uint64_t);
    } else {
        printf("Unknown length encoding %d in rdbLoadLen()", type);
        return -1; /* Never reached. */
    }
    return 0;
}

int proc_sync150(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
    std::vector<std::string> kvs;
    int ret = 0;

    while (link->input->size() > 1) {
        char *data = link->input->data();
        int size = link->input->size();
        int oper_offset = 0, size_offset = 0;
        uint64_t oper_len = 0, size_len = 0;
        if (ssdb_load_len(data, &oper_offset, &oper_len) == -1) {
            return -1;
        } else if (size < ((int) oper_len + oper_offset)) {
            link->input->grow();
            break;
        }
        std::string oper;
        oper.append(data + oper_offset, oper_len);
        data += (oper_offset + (int) oper_len);
        size -= (oper_offset + (int) oper_len);

        if (oper == "mset") {
            if (ssdb_load_len(data, &size_offset, &size_len) == -1) {
                return -1;
            } else if (size < ((int) size_offset + size_len)) {
                link->input->grow();
                break;
            }

            std::string str_local_size;
            str_local_size.append(data + size_offset, size_len);
            data += (size_offset + (int) size_len);
            size -= (size_offset + (int) size_len);

            long long n_local_size = 0;
            string2ll(str_local_size.c_str(), str_local_size.size(), &n_local_size);

            if (size < (int) n_local_size) {
                link->input->grow();
                break;
            }

            while (n_local_size > 0) {
                int key_offset = 0, val_offset = 0;
                uint64_t key_len = 0, val_len = 0;

                if (ssdb_load_len(data, &key_offset, &key_len) == -1) {
                    return -1;
                }
                std::string key;
                key.append(data + key_offset, key_len);
                data += (key_offset + (int) key_len);
                size -= (key_offset + (int) key_len);
                n_local_size -= (key_offset + (int) key_len);

                if (ssdb_load_len(data, &val_offset, &val_len) == -1) {
                    return -1;
                }
                std::string value;
                value.append(data + val_offset, val_len);
                data += (val_offset + (int) val_len);
                size -= (val_offset + (int) val_len);
                n_local_size -= (val_offset + (int) val_len);

                kvs.push_back(key);
                kvs.push_back(value);
            }
            link->input->decr(link->input->size() - size);

        } else if (oper == "complete") {
            link->input->decr(link->input->size() - size);
            resp->push_back("ok");
        }
    }

    ret = serv->ssdb->parse_replic(kvs);
    if (resp->size() > 0) {
        if (serv->ssdb->expiration != nullptr) {
            serv->ssdb->expiration->start();
        }
        serv->ssdb->start();
    }

    return ret;
}

int proc_rr_check_write(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back("rr_check_write ok");
    return 0;
}

int proc_rr_flushall_check(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    resp->push_back("ok");
    resp->push_back("rr_flushall_check ok");
    return 0;
}


int proc_rr_do_flushall(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;

	log_warn("[!!!] do flushall");
	std::queue<TransferJob *> discarded_jobs = net->redis->discard();

	log_warn("[!!!] discard %d TransferJob waiting for remain jobs", discarded_jobs.size());
	while (!discarded_jobs.empty())
	{
		delete discarded_jobs.front();
		discarded_jobs.pop();
	}

	Locking<RecordMutex<Mutex>> gl(&serv->transfer_mutex_record_);

	log_warn("[!!!] TransferJob clear done , starting flushdb");

    if (serv->ssdb->expiration != nullptr) {
        Locking<Mutex> l(&serv->ssdb->expiration->mutex);
        serv->ssdb->expiration->clear();
    }

    int ret = serv->ssdb->flushdb();
	if (ret < 0) {
		resp->push_back("ok");
		resp->push_back("rr_do_flushall nok");
	} else {
		resp->push_back("ok");
		resp->push_back("rr_do_flushall ok");
	}

    return 0;
}

int proc_rr_make_snapshot(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
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

int proc_rr_transfer_snapshot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
    SSDBServer *serv = (SSDBServer *) net->data;
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

    ReplicationJob *job = new ReplicationJob(serv, HostAndPort{ip, port}, link);
    net->replication->push(job);

    resp->resp.clear(); //prevent send resp
    return PROC_BACKEND;
}

int proc_rr_del_snapshot(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;

    {
        Locking<Mutex> l(&serv->replicMutex);

        if(serv->replicState == REPLIC_TRANS) {
            log_error("The replication is not finish");
            reply_errinfo_return("rr_del_snapshot error");
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
