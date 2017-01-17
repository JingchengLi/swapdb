//
// Created by zts on 17-1-3.
//

#include <memory>
#include <serv.h>


void *BackgroundJob::thread_func(void *arg) {
    BackgroundJob *backgroudJob = (BackgroundJob *) arg;

    while (!backgroudJob->thread_quit) {
        backgroudJob->loop(backgroudJob->serv->rt_bqueue);
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

    size_t qsize = serv->bqueue.size();

    if (qsize == 0) {
        return; //one thread only
    }

    if ((time_ms() - last) > 3000) {
        last = time_ms();
        if (qsize > 150) {
            log_error("BackgroundJob queue size is now : %d", qsize);
        } else if (qsize > 100) {
            log_warn("BackgroundJob queue size is now : %d", qsize);
        } else if (qsize > 50) {
            log_info("BackgroundJob queue size is now : %d", qsize);
        }
    }


    BTask bTask = serv->bqueue.pop();

    std::map<uint16_t, bproc_t>::iterator iter;

    iter = bproc_map.find(bTask.type);
    if (iter != bproc_map.end()) {
        log_debug("processing %d :%s", bTask.type, hexmem(bTask.data_key.data(), bTask.data_key.length()).c_str());

        PTST(bTask_process, 0.2)
        iter->second(serv, bTask.data_key, bTask.value);
        std::string res = bTask.dump();
        PTE(bTask_process, res)

    } else {
        log_error("can not find a way to process type:%d", bTask.type);
        //not found
        //TODO DEL
    }
}


bool BackgroundJob::proc(const std::string &data_key, const std::string &key, void *value, uint16_t type) {

    std::map<uint16_t, bproc_t>::iterator iter;
    iter = bproc_map.find(type);
    if (iter != bproc_map.end()) {
        log_debug("processing %d :%s", type, hexmem(data_key.data(), data_key.length()).c_str());
        iter->second(serv, data_key, value);
        //free value here~
        if (value != nullptr) {
            delete value;
        }
    } else {
        log_error("can not find a way to process type:%d", type);
        //not found
        //TODO DEL
    }

    return true;
}

void BackgroundJob::regType() {

//    REG_BPROC(COMMAND_DATA_SAVE);

    this->bproc_map[COMMAND_DATA_SAVE] = bproc_COMMAND_DATA_SAVE;
    this->bproc_map[COMMAND_DATA_DUMP] = bproc_COMMAND_DATA_DUMP;
}


int bproc_COMMAND_DATA_SAVE(SSDBServer *serv, const std::string &data_key, void *value) {

    DumpData *dumpData = (DumpData *) value;

    int64_t ttl;

    std::string val;

    PTST(rr_restore, 0.1)
    int ret = serv->ssdb->restore(dumpData->key, dumpData->expire, dumpData->data, dumpData->replace, &val);
    PTE(rr_restore, "")


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

    log_debug("[request2redis] : %s", hexstr<std::string>(str(req)).c_str());

    PTST(redisRequest, 0.1);
    auto t_res = link->redisRequest(req);
    PTE(redisRequest, hexstr<std::string>(str(req)));


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

    PTST(ssdb_dump, 0.1);
    int ret = serv->ssdb->dump(data_key, &val);
    PTE(ssdb_dump, data_key);

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
//    if (link == nullptr) {
//        log_error("link is null");
//        return -1;
//    }

    auto tl = unique_ptr<Link>(link);

    std::vector<std::string> req;
    req.push_back("customized-restore");
    req.push_back(data_key);
    req.push_back(str(pttl));
    req.push_back(val);
    req.push_back("replace");

    log_debug("[request2redis] : %s", hexstr<std::string>(str(req)).c_str());

    PTST(redisRequest, 0.2);
    auto t_res = link->redisRequest(req);
    PTE(redisRequest, hexstr<std::string>(str(req)));

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