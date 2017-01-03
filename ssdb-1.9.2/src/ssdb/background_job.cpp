//
// Created by zts on 17-1-3.
//

#include <memory>
#include "background_job.h"



void *BackgroudJob::thread_func(void *arg) {
    BackgroudJob *backgroudJob = (BackgroudJob *) arg;


    while (!backgroudJob->thread_quit) {

        backgroudJob->loop();

        if (backgroudJob->queued == 0) {
            usleep(1000 * 1000);
            log_info("BackgroudJob");
        }

    }

    log_debug("BackgroudJob thread quit");
    backgroudJob->thread_quit = false;

    return nullptr;
}


void BackgroudJob::start() {
    this->regType();
    thread_quit = false;
    pthread_t tid;
    int err = pthread_create(&tid, NULL, &BackgroudJob::thread_func, this);
    if (err != 0) {
        log_fatal("can't create thread: %s", strerror(err));
        exit(0);
    }
}

void BackgroudJob::stop() {
    thread_quit = true;
    for (int i = 0; i < 100; i++) {
        if (!thread_quit) {
            break;
        }
        usleep(10 * 1000);
    }
}


void BackgroudJob::loop() {

    std::string start;
    start.append(1, DataType::BQUEUE);

    auto it = std::unique_ptr<BIterator>(new BIterator(ssdb->iterator(start, "", 10))); //  +
    int n = 0;
    while (it->next()) {
        if (this->proc(it->key, it->value, it->type)) {
            n++;
        };
    }

    if (n == 10) {
        queued = 1;
    } else if (n == 0) {
        queued = 0;
    } else {
        queued = n;
    }

}


bool BackgroudJob::proc(const std::string &key, const std::string &value, uint16_t type) {

    std::map<uint16_t, bproc_t>::iterator iter;
    iter = bproc_map.find(type);
    if (iter != bproc_map.end()) {
        iter->second(ssdb, options, key, value);
    } else {
        log_error("can not find a way to process type:%d", type);
        //not found
        //TODO DEL
    }

    return true;
}

void BackgroudJob::regType() {

//    REG_BPROC(COMMAND_REDIS_DEL);

    this->bproc_map[COMMAND_REDIS_DEL] = bproc_COMMAND_REDIS_DEL;
}


int bproc_COMMAND_REDIS_DEL(SSDB *ssdb, const RedisUpstream &options, const std::string &key, const std::string &value) {

    Link *link = Link::connect(options.ip.c_str(), options.port);

    std::vector<std::string> req;
    req.push_back("customized-del");
    req.push_back(key);

    auto t_res =link->redisRequest(req);
    std::string res = t_res->toString();
    dump(res.data(), res.size());

    delete t_res;
    delete link;

    return 0;
}



