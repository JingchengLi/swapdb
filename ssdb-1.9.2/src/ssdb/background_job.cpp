#include <memory>
#include <serv.h>

#define MAX_RETRY 5

int notifyFailedToRedis(RedisUpstream *redisUpstream, std::string responseCommand, std::string dataKey);

void BackgroundJob::regType() {

    REG_BPROC(COMMAND_DATA_SAVE);
    REG_BPROC(COMMAND_DATA_DUMP);
    REG_BPROC(COMMAND_SYNC_PREPARE1);

}


void *BackgroundJob::thread_func(void *arg) {
    BackgroundJob *backgroudJob = (BackgroundJob *) arg;

    while (!backgroudJob->thread_quit) {
        backgroudJob->loop(backgroudJob->serv->bqueue);
    }

    log_debug("BackgroundJob thread quit");
    backgroudJob->thread_quit = false;

    return nullptr;
}


void BackgroundJob::start() {
    last = time_ms();

    this->regType();
    thread_quit = false;
    int err;

    pthread_t tid;
    err = pthread_create(&tid, NULL, &BackgroundJob::thread_func, this);
    if (err != 0) {
        log_fatal("can't create thread: %s", strerror(err));
        exit(0);
    }

}

void BackgroundJob::stop() {
    thread_quit = true;
    for (int i = 0; i < 100; i++) {
        if (!thread_quit) {
            break;
        }
        usleep(10 * 1000);
    }
}


void BackgroundJob::loop(const BQueue<BTask> &queue) {
    BTask bTask = serv->bqueue.pop();

    if (bTask.type > COMMAND_MAX) {
        log_error("unknown command %s", bTask.dump().c_str());
    }
    if (bTask.retry > MAX_RETRY) {
        free(bTask.value);
        bTask.value = nullptr;
        log_error("max retry limit reached task %s ", bTask.dump().c_str());
        return;
    }

    int64_t current = time_ms();

    int res = bproc_map[bTask.type](serv, bTask.data_key, bTask.value);
    if (res != 0) {
        log_error("bg_job failed %s ", bTask.dump().c_str());
//        if (res == -2) {
//            //retry when res == -2
//            bTask.retry++;
//            serv->bqueue.push(bTask);
//            return;
//        }
    }

    avg_wait = ((current - bTask.ts) * 1.0 - avg_wait) * 1.0 / count * 1.0 + avg_wait;
    avg_process = ((time_ms() - current) * 1.0 - avg_process) * 1.0 / count * 1.0 + avg_process;
    count++;

    if (count > INT16_MAX) {
        count = 0; //reset count.
    }
    if ((current - last) > 2000) {
        size_t qsize = serv->bqueue.size();
        last = time_ms();
        if (qsize > 500) {
            log_error("BackgroundJob queue size is now : %d", qsize);
        } else if (qsize > 218) {
            log_warn("BackgroundJob queue size is now : %d", qsize);
        } else if (qsize > 36) {
            log_info("BackgroundJob queue size is now : %d", qsize);
        } else if (qsize > 6) {
            log_debug("BackgroundJob queue size is now : %d", qsize);
        }

        log_info("task avg wait %f ms", avg_wait);
        log_info("task avg process %f ms", avg_process);

        if ((current - bTask.ts) > 1000) {
            log_warn("task %s had waited %d ms", bTask.dump().c_str(), ((current - bTask.ts)));
        }
    }


    if (bTask.value != nullptr) {
        free(bTask.value);
        bTask.value = nullptr;
    }

}

int bproc_COMMAND_DATA_SAVE(SSDBServer *serv, const std::string &data_key, void *value) {

    DumpData *dumpData = (DumpData *) value;

    int64_t pttl = dumpData->expire;
    std::string val;
    int ret = serv->ssdb->restore(dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);
    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(serv->redisUpstream, "customized-dump", data_key);
    }

    if (ret > 0 && pttl > 0) {
        Locking l(&serv->expiration->mutex);
        ret = serv->expiration->expire(dumpData->key, pttl, TimeUnit::Millisecond);
    }
    if (ret < 0) {
        //notify failed
        serv->ssdb->del(dumpData->key);
        return notifyFailedToRedis(serv->redisUpstream, "customized-dump", data_key);
    }

    std::vector<std::string> req = {"customized-del", data_key};
    log_debug("[request->redis] : %s %s", req[0].c_str(), req[1].c_str());

    RedisResponse *t_res = serv->redisUpstream->sendCommand(req);
    if (t_res == nullptr) {
        log_error("[%s %s] redis response is null", req[0].c_str(), req[1].c_str());
        //redis res failed
        return -1;
    }

    log_debug("[response<-redis] : %s %s %s", req[0].c_str(), req[1].c_str(), t_res->toString().c_str());

    delete t_res;

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

int bproc_COMMAND_DATA_DUMP(SSDBServer *serv, const std::string &data_key, void *value) {

    std::string val;
    int ret = serv->ssdb->dump(data_key, &val);

    if (ret < 0) {
        //notify failed
        return notifyFailedToRedis(serv->redisUpstream, "customized-restore", data_key);

    } else if (ret == 0) {
        //notify key not found
        notifyFailedToRedis(serv->redisUpstream, "customized-restore", data_key);
        return 0;

    } else {

        int64_t pttl = serv->expiration->pttl(data_key, TimeUnit::Millisecond);
        if (pttl < 0) {
            pttl = 0; //not sure
        }


        std::vector<std::string> req = {"customized-restore", data_key, str(pttl), val, "replace"};
        log_debug("[request->redis] : %s %s", req[0].c_str(), req[1].c_str());

        RedisResponse *t_res = serv->redisUpstream->sendCommand(req);
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

int bproc_COMMAND_SYNC_PREPARE1(SSDBServer *serv, const std::string &data_key, void *value) {

    std::vector<std::string> req = {"customized-sync1", "done"};

    log_debug("[request->redis] : customized-sync1");

    auto t_res = serv->redisUpstream->sendCommand(req);
    if (t_res == nullptr) {
        log_error("t_res is null");
        return -1;

    }
    delete t_res;

    return 0;
}