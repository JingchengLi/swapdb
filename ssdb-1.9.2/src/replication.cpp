//
// Created by zts on 17-4-27.
//
#include "replication.h"
#include <util/thread.h>
#include <net/link.h>
#include <util/cfree.h>
#include "serv.h"

extern "C" {
#include <redis/rdb.h>
#include <redis/zmalloc.h>
#include <redis/lzf.h>
};

void send_error_to_redis(Link *link);

void moveBuffer(Buffer *dst, Buffer *src, bool compress);

void saveStrToBuffer(Buffer *buffer, const Bytes &fit);


int ReplicationByIterator::process() {
    log_info("ReplicationByIterator::process");


    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    Link *master_link = upstream;
    const leveldb::Snapshot *snapshot = nullptr;

    log_info("[ReplicationWorker] send snapshot to %s start!", hnp.String().c_str());
    {
        Locking<Mutex> l(&serv->replicState.rMutex);
        snapshot = serv->replicState.rSnapshot;

        if (snapshot == nullptr) {
            log_error("snapshot is null, maybe rr_make_snapshot not receive or error!");
            reportError();
            return -1;
        }
    }

    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    iterate_options.snapshot = snapshot;
    iterate_options.readahead_size = 4 * 1024 * 1024;

    std::unique_ptr<Iterator> fit = std::unique_ptr<Iterator>(serv->ssdb->iterator("", "", -1, iterate_options));

    Link *ssdb_slave_link = Link::connect((hnp.ip).c_str(), hnp.port);
    if (ssdb_slave_link == nullptr) {
        log_error("fail to connect to slave ssdb %s!", hnp.String().c_str());
        log_debug("replic send snapshot failed!");

        reportError();

        return -1;
    }

    ssdb_slave_link->noblock(false);
    ssdb_slave_link->send(std::vector<std::string>({"ssdb_sync"}));
    ssdb_slave_link->write();
    ssdb_slave_link->response();
    ssdb_slave_link->noblock(true);


    log_debug("[ReplicationWorker] prepare for event loop");
    unique_ptr<Fdevents> fdes = unique_ptr<Fdevents>(new Fdevents());

    fdes->set(master_link->fd(), FDEVENT_IN, 1, master_link); //open evin
    master_link->noblock(true);

    const Fdevents::events_t *events;
    ready_list_t ready_list;
    ready_list_t ready_list_2;
    ready_list_t::iterator it;

    std::unique_ptr<Buffer> buffer = std::unique_ptr<Buffer>(new Buffer(1024 * 1024));

    int64_t start = time_ms();

    uint64_t rawBytes = 0;
    uint64_t sendBytes = 0;
    uint64_t packageSize = MIN_PACKAGE_SIZE; //init 512k
    uint64_t totalKeys = serv->ssdb->size();
    uint64_t visitedKeys = 0;

    totalKeys = (totalKeys > 0 ? totalKeys : 1);


    int64_t lastHeartBeat = time_ms();
    while (!quit) {
        ready_list.swap(ready_list_2);
        ready_list_2.clear();

        int64_t ts = time_ms();

        if (heartbeat) {
            if ((ts - lastHeartBeat) > 5000) {
                if (!master_link->output->empty()) {
                    log_debug("master_link->output not empty , redis may blocked ?");
                }

                RedisResponse r("rr_transfer_snapshot continue");
                master_link->output->append(Bytes(r.toRedis()));
                if (master_link->append_reply) {
                    master_link->send_append_res(std::vector<std::string>({"check 0"}));
                }
                lastHeartBeat = ts;
                if (!master_link->output->empty()) {
                    fdes->set(master_link->fd(), FDEVENT_OUT, 1, master_link);
                }
            }
        }

        if (!ready_list.empty()) {
            // ready_list not empty, so we should return immediately
            events = fdes->wait(0);
        } else {
            events = fdes->wait(5);
        }

        if (events == nullptr) {
            log_fatal("events.wait error: %s", strerror(errno));

            reportError();
            delete ssdb_slave_link;

            return -1;
        }

        for (int i = 0; i < (int) events->size(); i++) {
            //processing
            const Fdevent *fde = events->at(i);
            Link *link = (Link *) fde->data.ptr;
            if (fde->events & FDEVENT_IN) {
                ready_list.push_back(link);
                if (link->error()) {
                    continue;
                }
                int len = link->read();
                if (len <= 0) {
                    log_debug("fd: %d, read: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
                    link->mark_error();
                    continue;
                }
            }
            if (fde->events & FDEVENT_OUT) {
                if (link->output->empty()) {
                    fdes->clr(link->fd(), FDEVENT_OUT);
                    continue;
                }

                ready_list.push_back(link); //push into ready_list
                if (link->error()) {
                    continue;
                }
                int len = link->write();
                if (len <= 0) {
//                if (len < 0) {
                    log_debug("fd: %d, write: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
                    link->mark_error();
                    continue;
                } else if (link == ssdb_slave_link) {
                    sendBytes = sendBytes + len;
                }
                if (link->output->empty()) {
                    fdes->clr(link->fd(), FDEVENT_OUT);
                }
            }
        }

        for (it = ready_list.begin(); it != ready_list.end(); it++) {
            Link *link = *it;
            if (link->error()) {
                log_warn("fd: %d, link broken, address:%lld", link->fd(), link);

                if (link == master_link) {
                    log_info("link to redis broken");
                } else if (link == ssdb_slave_link) {
                    log_info("link to slave ssdb broken");
                    send_error_to_redis(master_link);
                } else {
                    log_info("?????????????????????????????????WTF????????????????????????????????????????????????");
                }

                fdes->del(ssdb_slave_link->fd());
                fdes->del(master_link->fd());

                delete ssdb_slave_link;
                delete master_link;
                upstream = nullptr;

                {
                    //update replic stats

                    Locking<Mutex> l(&serv->replicState.rMutex);
                    serv->replicState.finishReplic(false);
                }

                return -1;
            }
        }

        if (ssdb_slave_link->output->size() > MAX_PACKAGE_SIZE) {
//            uint s = uint(ssdb_slave_link->output->size() * 1.0 / (MIN_PACKAGE_SIZE * 1.0)) * 500;
            log_info("delay for output buffer write slow~");
            usleep(500);
            packageSize = MIN_PACKAGE_SIZE; //reset 512k
            continue;
        } else {
            if (packageSize < (MAX_PACKAGE_SIZE / 2)) {
                packageSize = packageSize * 2;
            }
            if (packageSize > (MAX_PACKAGE_SIZE)) {
                packageSize = MAX_PACKAGE_SIZE;
            }

        }

        bool finish = true;
        while (fit->next()) {
            saveStrToBuffer(buffer.get(), fit->key());
            saveStrToBuffer(buffer.get(), fit->val());
            visitedKeys++;

            if (visitedKeys % 1000000 == 0) {
                log_info("[%05.2f%%] processed %llu keys so far",
                         100 * ((double) visitedKeys * 1.0 / totalKeys * 1.0),
                         visitedKeys);
            }


            if (buffer->size() > packageSize) {
                saveStrToBuffer(ssdb_slave_link->output, "mset");
                rawBytes += buffer->size();
                moveBuffer(ssdb_slave_link->output, buffer.get(), compress);
                int len = ssdb_slave_link->write();
                if (len > 0) { sendBytes = sendBytes + len; }

                if (!ssdb_slave_link->output->empty()) {
                    fdes->set(ssdb_slave_link->fd(), FDEVENT_OUT, 1, ssdb_slave_link);
                }
                finish = false;
                break;
            }
        }

        if (finish) {
            if (!buffer->empty()) {
                saveStrToBuffer(ssdb_slave_link->output, "mset");
                rawBytes += buffer->size();
                moveBuffer(ssdb_slave_link->output, buffer.get(), compress);
                int len = ssdb_slave_link->write();
                if (len > 0) { sendBytes = sendBytes + len; }

                if (!ssdb_slave_link->output->empty()) {
                    fdes->set(ssdb_slave_link->fd(), FDEVENT_OUT, 1, ssdb_slave_link);
                }
            }

            if (!ssdb_slave_link->output->empty()) {
                log_debug("wait for output buffer empty~");
                continue; //wait for buffer empty
            } else {
                break;
            }
        }
    }

    {
        //del from event loop
        fdes->del(ssdb_slave_link->fd());
        fdes->del(master_link->fd());
    }

    bool transFailed = false;

    {
        //write "complete" to slave_ssdb
        ssdb_slave_link->noblock(false);
        saveStrToBuffer(ssdb_slave_link->output, "complete");
        int len = ssdb_slave_link->write();
        if (len > 0) { sendBytes = sendBytes + len; }

        const std::vector<Bytes> *res = ssdb_slave_link->response();
        if (res != nullptr && !res->empty()) {
            std::string result = (*res)[0].String();

            if (result == "failed" || result == "error") {
                transFailed = true;
            }

            std::string ret;
            for_each(res->begin(), res->end(), [&ret](const Bytes &h) {
                ret.append(" ");
                ret.append(hexstr(h));
            });

            log_debug("%s~", ret.c_str());

        } else {
            transFailed = true;
        }

    }


    if (transFailed) {
        reportError();
        log_info("[ReplicationWorker] send snapshot to %s failed!!!!", hnp.String().c_str());
        log_debug("send rr_transfer_snapshot failed!!");
        delete ssdb_slave_link;
        return -1;
    }

    {
        //update replic stats
        Locking<Mutex> l(&serv->replicState.rMutex);
        serv->replicState.finishReplic(true);
    }

    log_info("[ReplicationWorker] send snapshot to %s finished!", hnp.String().c_str());
    log_debug("send rr_transfer_snapshot finished!!");
    log_info("replic procedure finish!");
    log_info("[ReplicationWorker] task stats : dataSize %s, sendByes %s, elapsed %s",
             bytesToHuman(rawBytes).c_str(),
             bytesToHuman(sendBytes).c_str(),
             timestampToHuman((time_ms() - start)).c_str()
    );
    delete ssdb_slave_link;
    return 0;

}


void ReplicationByIterator::reportError() {
    send_error_to_redis(upstream);
    SSDBServer *serv = (SSDBServer *) ctx.net->data;

    {
        Locking<Mutex> l(&serv->replicState.rMutex);
        serv->replicState.finishReplic(false);
    }
    delete upstream;
    upstream = nullptr; //reset
}

void send_error_to_redis(Link *link) {
    if (link != nullptr) {
        link->quick_send({"ok", "rr_transfer_snapshot unfinished"});
        log_error("send rr_transfer_snapshot error!!");
    }
}

std::string replic_save_len(uint64_t len) {
    std::string res;

    unsigned char buf[2];

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        res.append(1, buf[0]);
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        res.append(1, buf[0]);
        res.append(1, buf[1]);
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        res.append(1, buf[0]);
        uint32_t len32 = htobe32(len);
        res.append((char *) &len32, sizeof(uint32_t));
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        res.append(1, buf[0]);
        len = htobe64(len);
        res.append((char *) &len, sizeof(uint64_t));
    }
    return res;
}


void saveStrToBuffer(Buffer *buffer, const Bytes &fit) {
    string val_len = replic_save_len((uint64_t) (fit.size()));
    buffer->append(val_len);
    buffer->append(fit);
}

void moveBuffer(Buffer *dst, Buffer *src, bool compress) {

    size_t comprlen = 0, outlen = (size_t) src->size();

    /**
     * when src->size() is small , comprlen may longer than outlen , which cause lzf_compress failed
     * and lzf_compress return 0 , so :so
     * 1. incr outlen too prevent compress failure
     * 2. if comprlen is zero , we copy raw data and will not uncompress on salve
     *
     */
    if (outlen < 100) {
        outlen = 1024;
    }

    std::unique_ptr<void, cfree_delete<void>> out(malloc(outlen + 1));


#ifndef REPLIC_NO_COMPRESS
    if (compress) {
        comprlen = lzf_compress(src->data(), (unsigned int) src->size(), out.get(), outlen);
    } else {
        comprlen = 0;
    }
#else
    comprlen = 0;
#endif

    dst->append(replic_save_len((uint64_t) src->size()));
    dst->append(replic_save_len(comprlen));

    if (comprlen == 0) {
        dst->append(src->data(), src->size());
    } else {
        dst->append(out.get(), (int) comprlen);
    }

    src->decr(src->size());
    src->nice();
}