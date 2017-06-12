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
    this->avg_process = 0.0;
    this->count = 1;
    this->last = 0;
}

int TransferWorker::proc(TransferJob *job) {

    if (redisUpstream == nullptr) {
        auto serv = ((SSDBServer* )(job->ctx.net->data));
        if (serv->opt.upstream_port == 0) {
            log_error("redis upstream conf is null");
            return -1;
        }
        this->redisUpstream = new RedisUpstream(serv->opt.upstream_ip, serv->opt.upstream_port);
        this->redisUpstream->reset();
    }

    if (this->redisUpstream->getLink() == nullptr) {
        log_error("cannot connect to redis");
        return -1;
    }

    int64_t current = time_ms();
    int res = (*job->proc)(job->ctx, this, job->data_key, job->value);
    if (res != 0) {
        log_error("bg_job failed %s ", job->dump().c_str());
    }

    int64_t process_time = time_ms() - current;
    int64_t wait_time = current -  job->ts;

    if (process_time > 100) {
        log_warn("task %s process %d ms",  job->dump().c_str(), process_time);
    }

    if (wait_time > 100) {
        log_debug("task %s had waited %d ms",  job->dump().c_str(), wait_time);
    }

    avg_wait = (wait_time * 1.0 - avg_wait) * 1.0 / (count * 1.0) + avg_wait;
    avg_process = (process_time * 1.0 - avg_process) * 1.0 / (count * 1.0) + avg_process;
    count++;

    if (count > 10000) {
        count = 1; //reset count.
        avg_wait = current - job->ts;
        avg_process = process_time;
    }

    if ((current - last) > 2000) {
         last = time_ms();

        log_info("trans_task avg w:%f p:%f", avg_wait, avg_process);
        if (wait_time > 100) {
            log_warn("task %s had waited %d ms",  job->dump().c_str(), wait_time);
        }

    }


    if (job->value != nullptr) {
        free(job->value);
        job->value = nullptr;
    }

    return 0;
}

