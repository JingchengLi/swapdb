/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_TRANSFERWORKER_H
#define SSDB_TRANSFERWORKER_H


#include <util/thread.h>
#include <include.h>
#include <sstream>
#include <util/strings.h>
#include <map>
#include <net/proc.h>
#include <util/dump_data.h>
#include "string"
#include "iostream"
#include "redis_stream.h"


typedef int (*bproc_t)(Context &ctx, TransferWorker *, const std::string &data_key, const std::string &trans_id, void *value);

class SSDBServer;


class TransferJob {
public:

    Context ctx;
    uint16_t type;
    int64_t ts;

    int retry;
    std::string data_key;

    std::string trans_id;

    DumpData *dumpData;
    bproc_t proc;

    TransferJob(Context &ctx, uint16_t type, const std::string &key, const std::string &id, DumpData *value = nullptr) :
            ctx(ctx), type(type), data_key(key),  trans_id(id), dumpData(value) {
        ts = time_ms();
        retry = 0;
    }

    std::string dump() {
        std::ostringstream stringStream;
        stringStream << " task_type:" << type
                     << " data_key: " << hexmem(data_key.data(), data_key.size()).c_str();
        return stringStream.str();
    }

    virtual ~TransferJob() {
        if (dumpData != nullptr) {
            delete dumpData;
            dumpData = nullptr;
        }
    }

};

// WARN: pipe latency is about 20 us, it is really slow!
class TransferWorker : public WorkerPool<TransferWorker, TransferJob *>::Worker {
public:

    TransferWorker(const std::string &name);

    void init();

    int proc(TransferJob *job);

    virtual ~TransferWorker();

    RedisUpstream *redisUpstream = nullptr;

private:

    //stat only
    int64_t count;
    double avg_wait;
    double avg_process;

    int64_t last;

};

typedef WorkerPool<TransferWorker, TransferJob *> TransferWorkerPool;


#endif //SSDB_TRANSFERWORKER_H
