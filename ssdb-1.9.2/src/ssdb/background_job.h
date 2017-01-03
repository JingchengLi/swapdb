//
// Created by zts on 17-1-3.
//

#ifndef SSDB_BACKGROUD_JOB_CPP_H
#define SSDB_BACKGROUD_JOB_CPP_H

#include <atomic>
#include "../util/thread.h"
#include "../util/log.h"
#include "ssdb.h"
#include <net/link.h>

#define COMMAND_REDIS_DEL 1


#define REG_BPROC(c)     this->bproc_map[##c] = bproc_##c
#define DEF_BPROC(c)     int bproc_##c(SSDB *ssdb, const RedisUpstream &options, const std::string &key, const std::string &value)



DEF_BPROC(COMMAND_REDIS_DEL);

typedef int (*bproc_t)(SSDB *ssdb, const RedisUpstream &options, const std::string &key, const std::string &value);


class BackgroudJob {
public:
    BackgroudJob(SSDB *ssdb, const RedisUpstream &options) : ssdb(ssdb), options(options) {
        this->thread_quit = false;
        start();
    }

    virtual ~BackgroudJob() {
        Locking l(&this->mutex);
        stop();
        ssdb = nullptr;
    }

    std::atomic<int> queued;

private:
    static void *thread_func(void *arg);

    Mutex mutex;

    SSDB *ssdb;
    const RedisUpstream &options;

    std::atomic<bool> thread_quit;

    std::map<uint16_t, bproc_t> bproc_map;


    void start();

    void stop();

    void loop();

    bool proc(const std::string &key, const std::string &value, uint16_t type);

    void regType();
};


#endif //SSDB_BACKGROUD_JOB_CPP_H
