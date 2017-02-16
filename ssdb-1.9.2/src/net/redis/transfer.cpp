//
// Created by zts on 17-2-15.
//

#include <util/log.h>
#include <serv.h>
#include "transfer.h"


TransferWorker::TransferWorker(const std::string &name) {
    this->name = name;
}

TransferWorker::~TransferWorker() {
    if (redisUpstream != nullptr) {
        delete redisUpstream;
    }
}

void TransferWorker::init() {
    log_debug("%s %d init", this->name.c_str(), this->id);
    this->avg_wait = 0.0;
    this->count = 1;
}

int TransferWorker::proc(TransferJob *job) {

    if (redisUpstream == nullptr) {
        RedisConf *conf = job->serv->redisConf;
        if (conf == nullptr) {
            log_error("redis upstream conf is null");
            return -1;
        }
        this->redisUpstream = new RedisUpstream(conf->ip.c_str(), conf->port);
    }

    int64_t current = time_ms();
    int res = (*job->proc)(job->serv, this, job->data_key, job->value);
    if (res != 0) {
        log_error("bg_job failed %s ", job->dump().c_str());
    }

    avg_wait = ((current - job->ts) * 1.0 - avg_wait) * 1.0 / count * 1.0 + avg_wait;
    avg_process = ((time_ms() - current) * 1.0 - avg_process) * 1.0 / count * 1.0 + avg_process;
    count++;

    if (count > INT16_MAX) {
        count = 0; //reset count.
    }

    if ((current - last) > 100) {
         last = time_ms();

        log_info("task avg wait %f ms", avg_wait);
        log_info("task avg process %f ms", avg_process);

        if ((current -  job->ts) > 1000) {
            log_warn("task %s had waited %d ms",  job->dump().c_str(), ((current -  job->ts)));
        }
    }


    if (job->value != nullptr) {
        free(job->value);
        job->value = nullptr;
    }

    return 0;
}

