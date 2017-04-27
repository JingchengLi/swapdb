//
// Created by zts on 17-4-27.
//

#ifndef SSDB_REPLICATION_H
#define SSDB_REPLICATION_H


#include <include.h>
#include <string>
#include <sstream>
#include <util/strings.h>
#include <util/thread.h>

class SSDBServer;
class SlaveInfo;


class ReplicationJob {
public:
    SSDBServer *serv;
    SlaveInfo  *slaveInfo;
    int64_t ts  ;


    ReplicationJob(SSDBServer *serv, SlaveInfo *slaveInfo) : serv(serv), slaveInfo(slaveInfo) {
        ts = time_ms();
    }

    virtual ~ReplicationJob() {
        delete slaveInfo;
    }


};

class ReplicationWorker : public WorkerPool<ReplicationWorker, ReplicationJob *>::Worker {
public:

    ReplicationWorker(const std::string &name);

    void init();

    int proc(ReplicationJob *job);

    virtual ~ReplicationWorker();

private:

};

typedef WorkerPool<ReplicationWorker, ReplicationJob *> ReplicationWorkerPool;

#endif //SSDB_REPLICATION_H
