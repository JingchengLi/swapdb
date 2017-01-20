#include <memory>
#include <serv.h>


void BackgroundJob::regType() {

    REG_BPROC(COMMAND_DATA_SAVE);
    REG_BPROC(COMMAND_DATA_DUMP);

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

    int64_t current = time_ms();

    bproc_map[bTask.type](serv, bTask.data_key, bTask.value);

    avg_wait = ((current -  bTask.ts)*1.0 - avg_wait)*1.0 / count * 1.0 + avg_wait;
    avg_process = ((time_ms() -  current)*1.0 - avg_process)*1.0 / count * 1.0 + avg_process;

    count++;

    if (bTask.value != nullptr) {
        free(bTask.value);
    }

//    std::map<uint16_t, bproc_t>::const_iterator iter;
//    iter = bproc_map.find(bTask.type);
//    if (iter != bproc_map.end()) {
//        log_debug("processing %s", bTask.dump().c_str());
//
//        PTST(bTask_process, 0.01)
//        iter->second(serv, bTask.data_key, bTask.value);
//        PTE(bTask_process, bTask.data_key)
//
//    } else {
//        log_error("can not find a way to process type:%d", bTask.type);
//        //not found
//        //TODO DEL
//    }
//

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
            log_warn("task %s had waited %d ms",bTask.dump().c_str() , ((current - bTask.ts)));
        }
    }


}


int bproc_COMMAND_DATA_SAVE(SSDBServer *serv, const std::string &data_key, void *value) {

    DumpData *dumpData = (DumpData *) value;

    int64_t ttl = dumpData->expire;

    std::string val;

    int ret = serv->ssdb->restore(dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);

    if (ret > 0 && ttl > 0) {
        Locking l(&serv->expiration->mutex);
        ret = serv->expiration->expire(dumpData->key, ttl, TimeUnit::Millisecond);
    }

    if (ret < 0) {
        log_info("%s : %s", hexmem(dumpData->key.data(), dumpData->key.size()).c_str(),
                 hexmem(dumpData->data.data(), dumpData->data.size()).c_str());
        return -1;
    }

    Link *link = serv->redisUpstream->getLink();
//    Link *link = serv->redisUpstream->getTmpLink();
//    auto tl = unique_ptr<Link>(link);

    if (link == nullptr) {
        log_error("link is null");
        return -1;
    }

    std::vector<std::string> req;
    req.push_back("customized-del");
    req.push_back(data_key);

    log_debug("[request2redis] : customized-del %s", data_key.c_str());

    PTST(redisRequest, 0.01);
    auto t_res = link->redisRequest(req);
    PTE(redisRequest, "customized-del "+data_key);


    if (t_res == nullptr) {
        log_error("t_res is null");
        serv->redisUpstream->reset();
        return -1;

    }

    std::string res = t_res->toString();
    log_debug("[response2redis] : %s", hexstr<std::string>(res).c_str());

    delete t_res;

    return 0;
}


int bproc_COMMAND_DATA_DUMP(SSDBServer *serv, const std::string &data_key, void *value) {

    std::string val;

    int ret = serv->ssdb->dump(data_key, &val);

    if (ret < 1) {
        //TODO
        log_error("bproc_COMMAND_DATA_DUMP error %d", ret);
        return -1;
    }

    int64_t pttl = serv->expiration->pttl(data_key, TimeUnit::Millisecond);
    if (pttl < 0) {
        pttl = 0;
    }


    Link *link = serv->redisUpstream->getLink();
//    Link *link = serv->redisUpstream->getTmpLink();

//    auto tl = unique_ptr<Link>(link);

    if (link == nullptr) {
        log_error("link is null");
        return -1;
    }

    std::vector<std::string> req;
    req.push_back("customized-restore");
    req.push_back(data_key);
    req.push_back(str(pttl));
    req.push_back(val);
    req.push_back("replace");

    log_debug("[request2redis] : customized-restore %s", data_key.c_str());

    PTST(redisRequest, 0.01);
    auto t_res = link->redisRequest(req);
    PTE(redisRequest, "customized-restore "+data_key);

    if (t_res == nullptr) {
        log_error("t_res is null");
        serv->redisUpstream->reset();
        return -1;

    }
    std::string res = t_res->toString();
    log_debug("[response2redis] : %s", hexstr<std::string>(res).c_str());


    if (t_res->status == 1 && t_res->str == "OK") {
        serv->ssdb->del(data_key);
    }

    delete t_res;

    return 0;


}