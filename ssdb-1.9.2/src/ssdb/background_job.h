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
#include <util/blocking_queue.h>
#include "background_job.h"

class SSDBServer;

#define _STRING(x) x
#define REG_BPROC(c)     this->bproc_map[_STRING(c)] = bproc_##c
#define DEF_BPROC(c)     int bproc_##c(SSDBServer *serv, const std::string &data_key, void* value)


#define COMMAND_DATA_SAVE 1
#define COMMAND_DATA_DUMP 2
#define COMMAND_SYNC_PREPARE1 2

DEF_BPROC(COMMAND_DATA_SAVE);
DEF_BPROC(COMMAND_DATA_DUMP);
DEF_BPROC(COMMAND_SYNC_PREPARE1);


typedef int (*bproc_t)(SSDBServer *serv, const std::string &data_key, void *value);


class BackgroundJob {
public:
    BackgroundJob(SSDBServer *serv) {
        this->serv = serv;
        this->thread_quit = false;
        this->avg_wait = 0.0;
        this->count = 1;
        start();
    }

    virtual ~BackgroundJob() {
        Locking l(&this->mutex);
        stop();
        serv = nullptr;
    }

private:

    int64_t count;
    double avg_wait;
    double avg_process;

    int64_t last;

    static void *thread_func(void *arg);

    Mutex mutex;

    SSDBServer *serv;

    std::atomic<bool> thread_quit;

    std::map<uint16_t, bproc_t> bproc_map;

    void start();

    void stop();

    void loop(const BQueue<BTask> &queue);

    void regType();

};


#endif //SSDB_BACKGROUD_JOB_CPP_H
