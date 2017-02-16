//
// Created by zts on 17-2-16.
//
#include <net/redis/redis_stream.h>
#include <net/redis/transfer.h>
#include "serv.h"

int notifyFailedToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey) ;


int bproc_COMMAND_DATA_SAVE(SSDBServer *serv, TransferWorker *worker, const std::string &data_key, void *value) {

    DumpData *dumpData = (DumpData *) value;

    int64_t pttl = dumpData->expire;
    std::string val;
    int ret = serv->ssdb->restore(dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);
    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, "customized-dump", data_key);
    }

    if (ret > 0 && pttl > 0) {
        Locking l(&serv->expiration->mutex);
        ret = serv->expiration->expire(dumpData->key, pttl, TimeUnit::Millisecond);
    }
    if (ret < 0) {
        //notify failed
        serv->ssdb->del(dumpData->key);
        return notifyFailedToRedis(worker->redisUpstream, "customized-dump", data_key);
    }

    std::vector<std::string> req = {"customized-del", data_key};
    log_debug("[request->redis] : %s %s", req[0].c_str(), req[1].c_str());

    RedisResponse *t_res = worker->redisUpstream->sendCommand(req);
    if (t_res == nullptr) {
        log_error("[%s %s] redis response is null", req[0].c_str(), req[1].c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s %s", req[0].c_str(), req[1].c_str(), t_res->toString().c_str());

    delete t_res;

    return 0;
}

int bproc_COMMAND_DATA_DUMP(SSDBServer *serv, TransferWorker *worker, const std::string &data_key, void *value) {

    std::string val;
    int ret = serv->ssdb->dump(data_key, &val);

    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(worker->redisUpstream, "customized-restore", data_key);

    } else if (ret == 0) {
        //notify key not found
        notifyFailedToRedis(worker->redisUpstream, "customized-restore", data_key);
        return 0;

    } else {

        int64_t pttl = serv->expiration->pttl(data_key, TimeUnit::Millisecond);
        if (pttl < 0) {
            pttl = 0; //not sure
        }


        std::vector<std::string> req = {"customized-restore", data_key, str(pttl), val, "replace"};
        log_debug("[request->redis] : %s %s", req[0].c_str(), req[1].c_str());

        RedisResponse *t_res = worker->redisUpstream->sendCommand(req);
        if (t_res == nullptr) {
            log_error("[%s %s] redis response is null", req[0].c_str(), req[1].c_str());
            //redis res failed
            return -1;
        }

        log_debug("[response<-redis] : %s %s %s", req[0].c_str(), req[1].c_str(), t_res->toString().c_str());


        if (t_res->isOk()) {
            serv->ssdb->del(data_key);
        }

        delete t_res;
    }


    return 0;
}





int notifyFailedToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey) {
    std::vector<std::string> req = {"customized-fail", responseCommand, dataKey};
    log_debug("[request->redis] : %s %s %s", req[0].c_str(), req[1].c_str(), req[2].c_str());

    RedisResponse *t_res = redisUpstream->sendCommand(req);
    if (t_res == nullptr) {
        log_error("[%s %s %s] redis response is null", req[0].c_str(), req[1].c_str(), req[2].c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s  %s %s", req[0].c_str(), req[1].c_str(), req[2].c_str(), t_res->toString().c_str());

    delete t_res;
    return -1;
}