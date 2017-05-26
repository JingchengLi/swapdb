//
// Created by zts on 17-2-16.
//
#include <net/redis/redis_stream.h>
#include <net/redis/transfer.h>
#include "serv.h"

int notifyFailedToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey);
int notifyNotFoundToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey);


int bproc_COMMAND_DATA_SAVE(Context &ctx, TransferWorker *worker, const std::string &data_key, void *value) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const std::string cmd = "ssdb-resp-dump";
    const std::string del_cmd = "ssdb-resp-del";

    RecordLock<Mutex> l(&serv->transfer_mutex_record_, data_key);

    DumpData *dumpData = (DumpData *) value;

    std::string val;

    PTST(restore, 0.03)
    int ret = serv->ssdb->restore(ctx, dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);
    PTE(restore, hexstr(data_key))

    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, cmd, data_key);
    }

    std::vector<std::string> req = {del_cmd, data_key};
    log_debug("[request->redis] : %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str());

    std::unique_ptr<RedisResponse> t_res(worker->redisUpstream->sendCommand(req));
    if (!t_res) {
        log_error("[%s %s] redis response is null", hexstr(req[0]).c_str(), hexstr(req[1]).c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), t_res->toString().c_str());

    return 0;
}

int bproc_COMMAND_DATA_DUMP(Context &ctx, TransferWorker *worker, const std::string &data_key, void *value) {
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    const std::string cmd = "ssdb-resp-restore";

    RecordLock<Mutex> l(&serv->transfer_mutex_record_, data_key);

    std::string val;

    PTST(dump, 0.03)
    int ret = serv->ssdb->dump(ctx, data_key, &val);
    PTE(dump, hexstr(data_key))


    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, cmd, data_key);

    } else if (ret == 0) {
        //notify key not found
        notifyNotFoundToRedis(worker->redisUpstream, cmd, data_key);
        return 0;

    } else {

        int64_t pttl = serv->ssdb->expiration->pttl(ctx, data_key, TimeUnit::Millisecond);
        if (pttl < 0) {
            pttl = 0; //not sure
        }


        std::vector<std::string> req = {cmd, data_key, str(pttl), val, "replace"};
        log_debug("[request->redis] : %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str());

        std::unique_ptr<RedisResponse> t_res(worker->redisUpstream->sendCommand(req));
        if (!t_res) {
            log_error("[%s %s] redis response is null", hexstr(req[0]).c_str(), hexstr(req[1]).c_str());
            //redis res failed
            return -1;
        }

        log_debug("[response<-redis] : %s %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), t_res->toString().c_str());


        if (t_res->isOk()) {
            serv->ssdb->del(ctx, data_key);
        }

    }


    return 0;
}



int notifyFailedToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey) {
    const std::string cmd = "ssdb-resp-fail";

    std::vector<std::string> req = {cmd, responseCommand, dataKey};
    log_debug("[request->redis] : %s %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str());

    std::unique_ptr<RedisResponse> t_res(redisUpstream->sendCommand(req));
    if (!t_res) {
        log_error("[%s %s %s] redis response is null", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s  %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str(),
              t_res->toString().c_str());

    return -1;
}

int notifyNotFoundToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey) {
    const std::string cmd = "ssdb-resp-notfound";

    std::vector<std::string> req = {cmd, responseCommand, dataKey};
    log_debug("[request->redis] : %s %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str());

    std::unique_ptr<RedisResponse> t_res(redisUpstream->sendCommand(req));
    if (!t_res) {
        log_error("[%s %s %s] redis response is null", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s  %s %s", hexstr(req[0]).c_str(), hexstr(req[1]).c_str(), hexstr(req[2]).c_str(),
              t_res->toString().c_str());

    return -1;
}
