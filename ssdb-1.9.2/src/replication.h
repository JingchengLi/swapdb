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
#include <common/context.hpp>

const uint64_t MAX_PACKAGE_SIZE = 8 * 1024 * 1024;
const uint64_t MIN_PACKAGE_SIZE = 1024 * 1024;

//#define REPLIC_NO_COMPRESS TRUE

void *ssdb_sync(void *arg);

int replic_decode_len(const char *data, int *offset, uint64_t *lenptr);
std::string replic_save_len(uint64_t len);


class SSDBServer;

class ReplicationJob {
public:
    Context ctx;
    HostAndPort hnp;
    Link *upstream;

    int64_t ts;

    bool heartbeat;

    volatile bool quit = false;

    ReplicationJob(const Context& ctx, const HostAndPort& hnp, Link *link, bool heartbeat = false) :
            ctx(ctx), hnp(hnp), upstream(link), heartbeat(heartbeat) {
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

    void reportError(ReplicationJob *job);
};

typedef WorkerPool<ReplicationWorker, ReplicationJob *> ReplicationWorkerPool;

#endif //SSDB_REPLICATION_H
