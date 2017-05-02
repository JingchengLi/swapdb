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
#include <net/redis/redis_client.h>

class SSDBServer;


class ReplicationJob {
public:
    SSDBServer *serv;
    HostAndPort hnp;
    Link *upstreamRedis;

    int64_t ts  ;

    volatile bool quit = false;

    ReplicationJob(SSDBServer *serv, const HostAndPort &hnp, Link *link) : serv(serv), hnp(hnp), upstreamRedis(link) {
        ts = time_ms();
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
