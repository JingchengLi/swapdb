//
// Created by zts on 17-1-3.
//

#include <memory>
#include "background_job.h"
#include <serv.h>


void *BackgroundJob::thread_func(void *arg) {
    BackgroundJob *backgroudJob = (BackgroundJob *) arg;


    while (!backgroudJob->thread_quit) {


        backgroudJob->loop();

        if (backgroudJob->queued == 0) {
            log_info("Background wait Job");
            backgroudJob->cv.wait();
        }

//        if (backgroudJob->queued == 0) {
//            usleep(1000 * 1000);
//            log_info("BackgroundJob");
//        }

    }

    log_debug("BackgroundJob thread quit");
    backgroudJob->thread_quit = false;

    return nullptr;
}


void BackgroundJob::start() {
    this->regType();
    thread_quit = false;
    pthread_t tid;
    int err = pthread_create(&tid, NULL, &BackgroundJob::thread_func, this);
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


void BackgroundJob::loop() {

    std::string start;
    start.append(1, DataType::BQUEUE);

    auto it = std::unique_ptr<BIterator>(new BIterator(serv->ssdb->iterator(start, "", -1))); //  +
    int n = 0;
    while (it->next()) {
        this->proc(it->data_key, it->key, it->value, it->type);
        n++;
    }

    queued = n;

}


bool BackgroundJob::proc(const std::string &data_key, const std::string &key, const std::string &value, uint16_t type) {

    std::map<uint16_t, bproc_t>::iterator iter;
    iter = bproc_map.find(type);
    if (iter != bproc_map.end()) {
        log_debug("processing %d :%s", type, hexmem(data_key.data(), data_key.length()).c_str());
        iter->second(serv, data_key, key, value);
    } else {
        log_error("can not find a way to process type:%d", type);
        //not found
        //TODO DEL
    }

    return true;
}

void BackgroundJob::regType() {

//    REG_BPROC(COMMAND_REDIS_DEL);

    this->bproc_map[COMMAND_REDIS_DEL] = bproc_COMMAND_REDIS_DEL;
    this->bproc_map[COMMAND_REDIS_RESTROE] = bproc_COMMAND_REDIS_RESTROE;
}


int bproc_COMMAND_REDIS_DEL(SSDBServer *serv, const std::string &data_key, const std::string &key,
                            const std::string &value) {

    Link *link = Link::connect(serv->redisUpstream->ip.c_str(), serv->redisUpstream->port);
    if (link == nullptr) {
        log_error("link is null");
        return -1;
    }

    std::vector<std::string> req;
    req.push_back("customized-del");
    req.push_back(data_key);

    log_debug("send back to redis : %s", hexstr<std::string>(str(req)).c_str());

    auto t_res = link->redisRequest(req);
    if (t_res == nullptr) {
        log_error("t_res is null");
        delete link;
        return -1;

    }
    std::string res = t_res->toString();
    log_debug("redis response : %s", hexstr<std::string>(res).c_str());

    serv->ssdb->raw_del(key);

    delete t_res;
    delete link;

    return 0;
}


int bproc_COMMAND_REDIS_RESTROE(SSDBServer *serv, const std::string &data_key, const std::string &key,
                                const std::string &value) {

    std::string val;
    int ret = serv->ssdb->dump(data_key, &val);
    if (ret < 1) {
        log_error("bproc_COMMAND_REDIS_RESTROE error");
        return -1;
    }

    int64_t pttl = serv->expiration->pttl(data_key, TimeUnit::Millisecond);
    if (pttl < 0) {
        pttl = 0;
    }


    Link *link = Link::connect(serv->redisUpstream->ip.c_str(), serv->redisUpstream->port);
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

    log_debug("send back to redis : %s", hexstr<std::string>(str(req)).c_str());

    auto t_res = link->redisRequest(req);
    if (t_res == nullptr) {
        log_error("t_res is null");
        delete link;
        return -1;

    }
    std::string res = t_res->toString();
    log_debug("redis response : %s", hexstr<std::string>(res).c_str());


    serv->ssdb->raw_del(key);

    if (t_res->status == 1 && t_res->str == "OK") {
        serv->ssdb->del(key);
    }

    delete t_res;
    delete link;

    return 0;


}