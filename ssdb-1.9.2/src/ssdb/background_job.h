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

class SSDBServer;

#define COMMAND_REDIS_DEL 1
#define COMMAND_REDIS_RESTROE 2


#define REG_BPROC(c)     this->bproc_map[##c] = bproc_##c
#define DEF_BPROC(c)     int bproc_##c(SSDBServer *serv, const std::string &data_key, const std::string &key, const std::string &value)



DEF_BPROC(COMMAND_REDIS_DEL);
DEF_BPROC(COMMAND_REDIS_RESTROE);

typedef int (*bproc_t)(SSDBServer *serv, const std::string &data_key, const std::string &key, const std::string &value);


class BackgroundJob {
public:
    BackgroundJob(SSDBServer *serv) {
        this->cv = CondVar(&mutex);
        this->serv = serv;
        this->thread_quit = false;
        start();
    }

    virtual ~BackgroundJob() {
        Locking l(&this->mutex);
        stop();
        serv = nullptr;
    }

    std::atomic<int> queued;
    CondVar cv = CondVar(nullptr);

private:
    static void *thread_func(void *arg);

    Mutex mutex;

    SSDBServer *serv;

    std::atomic<bool> thread_quit;

    std::map<uint16_t, bproc_t> bproc_map;


    void start();

    void stop();

    void loop();

    bool proc(const std::string &data_key, const std::string &key, const std::string &value, uint16_t type);

    void regType();
};


#endif //SSDB_BACKGROUD_JOB_CPP_H
