/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <net/redis/redis_stream.h>
#include <net/redis/transfer.h>
#include "serv.h"

int notifyFailedToRedis(RedisUpstream *redisUpstream, const std::string &response_cmd, const std::string &data_key,
                        const std::string &trans_id);

int notifyNotFoundToRedis(RedisUpstream *redisUpstream, const std::string &response_cmd, const std::string &data_key,
                          const std::string &trans_id);

int notifyToRedis(RedisUpstream *redisUpstream, const std::string &response_type, const std::string &response_cmd,
                  const std::string &data_key, const std::string &trans_id);

int bproc_COMMAND_DATA_SAVE(Context &ctx, TransferWorker *worker, const std::string &data_key,
                            const std::string &trans_id, void *value) {

    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    RecordLock<Mutex> tl(&serv->transfer_mutex_record_, data_key);

    const std::string cmd = "ssdb-resp-dump";


    DumpData *dumpData = (DumpData *) value;

    std::string val;

    PTST(restore, 0.03)
    int ret = serv->ssdb->restore(ctx, dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);
    PTE(restore, hexstr(data_key))

    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, cmd, data_key, trans_id);
    }


    std::vector<std::string> req = {"ssdb-resp-del", data_key, trans_id};
    log_debug("[request->redis] : %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]));

    std::unique_ptr<RedisResponse> t_res(worker->redisUpstream->sendCommand(req));
    if (!t_res) {
        log_error("[%s %s %s] redis response is null", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]));
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]),
              t_res->toString().c_str());

    return 0;
}

int bproc_COMMAND_DATA_DUMP(Context &ctx, TransferWorker *worker, const std::string &data_key,
                            const std::string &trans_id, void *value) {

    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    RecordLock<Mutex> tl(&serv->transfer_mutex_record_, data_key);

    const std::string cmd = "ssdb-resp-restore";

    std::string val;

    int64_t pttl = 0;

    PTST(dump, 0.03)
    int ret = serv->ssdb->dump(ctx, data_key, &val, &pttl, serv->opt.rdb_compression);
    PTE(dump, hexstr(data_key))


    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, cmd, data_key, trans_id);

    } else if (ret == 0) {
        //notify key not found
        notifyNotFoundToRedis(worker->redisUpstream, cmd, data_key, trans_id);
        return 0;

    } else {

        //process restore to redis
        std::vector<std::string> req = {cmd, data_key, str(pttl), val, "replace", trans_id};
        log_debug("[request->redis] : %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[5]));

        std::unique_ptr<RedisResponse> t_res(worker->redisUpstream->sendCommand(req));
        if (!t_res) {
            log_error("[%s %s %s] redis response is null", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[5]));
            //redis res failed
            return -1;
        }

        log_debug("[response<-redis] : %s %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[5]),
                  t_res->toString().c_str());


        if (t_res->isOk()) {
            log_debug("mark deleting %s", hexcstr(data_key));
            serv->ssdb->del(ctx, data_key);
        }

    }


    return 0;
}


int notifyFailedToRedis(RedisUpstream *redisUpstream, const std::string &response_cmd, const std::string &data_key,
                        const std::string &trans_id) {
    return notifyToRedis(redisUpstream, "ssdb-resp-fail", response_cmd, data_key, trans_id);
}

int notifyNotFoundToRedis(RedisUpstream *redisUpstream, const std::string &response_cmd, const std::string &data_key,
                          const std::string &trans_id) {
    return notifyToRedis(redisUpstream, "ssdb-resp-notfound", response_cmd, data_key, trans_id);
}


int notifyToRedis(RedisUpstream *redisUpstream,
                  const std::string &response_type,
                  const std::string &response_cmd,
                  const std::string &data_key,
                  const std::string &trans_id) {


    std::vector<std::string> req = {response_type, response_cmd, data_key, trans_id};
    log_debug("[request->redis] : %s %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]), hexcstr(req[3]));

    std::unique_ptr<RedisResponse> t_res(redisUpstream->sendCommand(req));
    if (!t_res) {
        log_error("[%s %s %s %s] redis response is null", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]), hexcstr(req[3]));
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s  %s %s %s", hexcstr(req[0]), hexcstr(req[1]), hexcstr(req[2]), hexcstr(req[3]),
              t_res->toString().c_str());

    return -1;

}