//
// Created by zts on 17-4-27.
//
#include "replication.h"
#include <util/thread.h>
#include <net/link.h>
#include <redis/rdb.h>
#include "serv.h"


void send_error_to_redis(Link *link);
int ssdb_save_len(uint64_t len, std::string &res);
void prepareDataForLink(const Link *link, const unique_ptr<Buffer> &buffer);
void saveStrToBuffer(Buffer *buffer, const Bytes &fit);

ReplicationWorker::ReplicationWorker(const std::string &name) {
    this->name = name;
}

ReplicationWorker::~ReplicationWorker() {

}

void ReplicationWorker::init() {
}

int ReplicationWorker::proc(ReplicationJob *job) {
    SSDBServer *serv = job->serv;
    HostAndPort hnp = job->hnp;
    Link *master_link = job->upstreamRedis;
    
    if (serv->replicSnapshot == nullptr) {
        log_error("snapshot is null, maybe rr_make_snapshot not receive or error!");
        send_error_to_redis(master_link);
        return 0;
    }

    Link *link = Link::connect((hnp.ip).c_str(), hnp.port);

    if (link == nullptr) {
        log_error("fail to connect to slave ssdb! ip[%s] port[%d]", hnp.ip.c_str(), hnp.port);
        send_error_to_redis(master_link);
        return 0;
    }

    link->send(std::vector<std::string>({"sync150"}));
    link->write();
    link->response();

    std::unique_ptr<Buffer> buffer = std::unique_ptr<Buffer>(new Buffer(8 * 1024));
    std::string oper = "mset";
    std::string oper_len;
    ssdb_save_len((uint64_t) oper.size(), oper_len);
    const leveldb::Snapshot *snapshot = serv->replicSnapshot;
    auto fit = std::unique_ptr<Iterator>(serv->ssdb->iterator("", "", -1, snapshot));
    while (fit->next()) {
        saveStrToBuffer(buffer.get(), fit->key());
        saveStrToBuffer(buffer.get(), fit->val());

        if (buffer->size() > 1024) {
            link->output->append(oper_len);
            link->output->append(oper);

            prepareDataForLink(link, buffer);

            if (link->write() == -1) {
                log_error("fd: %d, send error: %s", link->fd(), strerror(errno));
                send_error_to_redis(master_link);
                delete link;
                return 0;
            }

            buffer->decr(buffer->size());
            buffer->nice();
        }
    }

    if (buffer->size() > 0) {
        link->output->append(oper_len);
        link->output->append(oper);

        prepareDataForLink(link, buffer);

        if (link->write() == -1) {
            log_error("fd: %d, send error: %s", link->fd(), strerror(errno));
            send_error_to_redis(master_link);
            delete link;
            return 0;
        }

        buffer->decr(buffer->size());
        buffer->nice();
    }

    saveStrToBuffer(link->output, "complete");
    link->write();
    link->read();
    delete link;

    {
        Locking<Mutex> l(&serv->replicMutex);
        serv->replicNumFinished++;
        if (serv->replicNumFinished == serv->replicNumStarted) {
            serv->replicState = REPLIC_END;
        }
    }

    log_debug("replic procedure finish!");

    return 0;
}


void send_error_to_redis(Link *link) {
    if (link != NULL) {
        link->send(std::vector<std::string>({"error" , "rr_transfer_snapshot error"}));
        if (link->append_reply) {
            link->send_append_res(std::vector<std::string>({"check 0"}));
        }
        link->write();
        log_error("send rr_transfer_snapshot error!!");
    }
}

int ssdb_save_len(uint64_t len, std::string &res) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        res.append(1, buf[0]);
        nwritten = 1;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        res.append(1, buf[0]);
        res.append(1, buf[1]);
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        res.append(1, buf[0]);
        uint32_t len32 = htobe32(len);
        res.append((char *) &len32, sizeof(uint32_t));
        nwritten = 1 + 4;
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        res.append(1, buf[0]);
        len = htobe64(len);
        res.append((char *) &len, sizeof(uint64_t));
        nwritten = 1 + 8;
    }
    return (int) nwritten;
}


void saveStrToBuffer(Buffer *buffer, const Bytes &fit) {
    string val_len;
    ssdb_save_len((uint64_t) (fit.size()), val_len);
    buffer->append(val_len);
    buffer->append(fit);
}

void prepareDataForLink(const Link *link, const unique_ptr<Buffer> &buffer) {
    char buf[32] = {0};
    ll2string(buf, sizeof(buf) - 1, (long long) buffer->size());
    string buffer_size(buf);
    string buffer_len;
    ssdb_save_len((uint64_t) buffer_size.size(), buffer_len);
    link->output->append(buffer_len);
    link->output->append(buffer_size);

    link->output->append(buffer->data(), buffer->size());
}