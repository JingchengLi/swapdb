//
// Created by zts on 17-4-27.
//
#include "replication.h"
#include <util/thread.h>
#include <net/link.h>
#include <util/cfree.h>
#include "serv.h"
#include <snappy.h>

extern "C" {
#include <redis/rdb.h>
#include <redis/zmalloc.h>
#include <redis/lzf.h>
};

static void send_error_to_redis(Link *link);

static void moveBufferSync(Buffer *dst, Buffer *src, bool compress);

static void moveBufferAsync(ReplicationByIterator2 *job, Buffer *dst, Buffer *src, bool compress);

static void saveStrToBuffer(Buffer *buffer, const Bytes &fit);


int ReplicationByIterator2::process() {
    log_info("ReplicationByIterator2::process");


    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    Link *master_link = upstream;
    const leveldb::Snapshot *snapshot = nullptr;

    log_info("[ReplicationByIterator2] send snapshot to %s start!", hnp.String().c_str());
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
    auto iterator_ptr = fit.get();

    Link *ssdb_slave_link = Link::connect((hnp.ip).c_str(), hnp.port);
    if (ssdb_slave_link == nullptr) {
        log_error("fail to connect to slave ssdb %s!", hnp.String().c_str());
        log_debug("replic send snapshot failed!");

        reportError();

        return -1;
    }

    ssdb_slave_link->noblock(false);
    std::vector<std::string> ssdb_sync_cmd({"ssdb_sync2"});
    if (heartbeat) {
        ssdb_sync_cmd.emplace_back("heartbeat");
        ssdb_sync_cmd.emplace_back("1");
    }

    ssdb_slave_link->send(ssdb_sync_cmd);
    ssdb_slave_link->write();
    ssdb_slave_link->response();
    ssdb_slave_link->noblock(true);

    log_info("[ReplicationByIterator2] ssdb_sync2 done");

    log_debug("[ReplicationByIterator2] prepare for event loop");
    unique_ptr<Fdevents> fdes = unique_ptr<Fdevents>(new Fdevents());

    fdes->set(master_link->fd(), FDEVENT_IN, 1, master_link); //open evin
    master_link->noblock(true);

    const Fdevents::events_t *events;
    ready_list_t ready_list;
    ready_list_t ready_list_2;
    ready_list_t::iterator it;


    int64_t start = time_ms();

    uint64_t rawBytes = 0;
    uint64_t sendBytes = 0;
    uint64_t packageSize = compress ? MAX_PACKAGE_SIZE : MIN_PACKAGE_SIZE; //init 512k
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
                    net_debug("fd: %d, read: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
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
                    net_debug("fd: %d, write: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
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

        if (ssdb_slave_link->output->size() > MAX_PACKAGE_SIZE * 3) {
//            uint s = uint(ssdb_slave_link->output->size() * 1.0 / (MIN_PACKAGE_SIZE * 1.0)) * 500;
            log_info("delay for output buffer write slow~");
            usleep(5000);
            continue;
        }

        bool finish = true;
        while (iterator_ptr->next()) {
            saveStrToBufferQuick(buffer, iterator_ptr->key());
            saveStrToBufferQuick(buffer, iterator_ptr->val());
            visitedKeys++;

            if (visitedKeys % 1000000 == 0) {
                log_info("[%05.2f%%] processed %llu keys so far , elapsed %s",
                         100 * ((double) visitedKeys * 1.0 / totalKeys * 1.0),
                         visitedKeys, timestampToHuman((time_ms() - start)).c_str()
                );
            }


            if (buffer->size() > packageSize) {
                rawBytes += buffer->size();

                moveBufferAsync(this, ssdb_slave_link->output, buffer, compress);
//                moveBufferSync(ssdb_slave_link->output, buffer, compress);


                if (!ssdb_slave_link->output->empty()) {
                    int len = ssdb_slave_link->write(-1);
                    if (len > 0) { sendBytes = sendBytes + len; }
                }

                if (!ssdb_slave_link->output->empty()) {
                    log_warn("slave slowdown ?");
                    fdes->set(ssdb_slave_link->fd(), FDEVENT_OUT, 1, ssdb_slave_link);
                }


                finish = false;
                break;
            }
        }

        if (finish) {
            moveBufferAsync(this, ssdb_slave_link->output, nullptr, compress);

            if (!buffer->empty()) {
                rawBytes += buffer->size();

                moveBufferSync(ssdb_slave_link->output, buffer, compress);

                if (!ssdb_slave_link->output->empty()) {
                    int len = ssdb_slave_link->write();
                    if (len > 0) { sendBytes = sendBytes + len; }
                }

                if (!ssdb_slave_link->output->empty()) {
                    log_warn("slave slowdown ?");
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
        log_info("[ReplicationByIterator2] send snapshot to %s failed!!!!", hnp.String().c_str());
        log_debug("send rr_transfer_snapshot failed!!");
        delete ssdb_slave_link;
        return -1;
    }

    {
        //update replic stats
        Locking<Mutex> l(&serv->replicState.rMutex);
        serv->replicState.finishReplic(true);
    }

    log_info("[ReplicationByIterator2] send snapshot to %s finished!", hnp.String().c_str());
    log_debug("send rr_transfer_snapshot finished!!");
    log_info("replic procedure finish!");
    log_info("[ReplicationByIterator2] task stats : dataSize %s, sendByes %s, elapsed %s",
             bytesToHuman(rawBytes).c_str(),
             bytesToHuman(sendBytes).c_str(),
             timestampToHuman((time_ms() - start)).c_str()
    );
    delete ssdb_slave_link;
    return 0;

}

void ReplicationByIterator2::saveStrToBufferQuick(Buffer *buffer, const Bytes &fit) {
    auto fit_size = fit.size();
    if (fit_size < quickmap_size) {
        buffer->append(quickmap[fit_size].data(), (int) quickmap[fit_size].size());
    } else {
        string val_len = replic_save_len((uint64_t) (fit_size));
        buffer->append(val_len);
    }
    buffer->append(fit.data(), fit_size);
}


void send_error_to_redis(Link *link) {
    if (link != nullptr) {
        link->quick_send({"ok", "rr_transfer_snapshot unfinished"});
        log_error("send rr_transfer_snapshot error!!");
    }
}


void saveStrToBuffer(Buffer *buffer, const Bytes &fit) {
    string val_len = replic_save_len((uint64_t) (fit.size()));
    buffer->append(val_len);
    buffer->append(fit);
}

void moveBufferSync(Buffer *dst, Buffer *src, bool compress) {
    saveStrToBuffer(dst, "mset");
    dst->append(replic_save_len((uint64_t) src->size()));

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


#ifndef REPLIC_NO_COMPRESS
    if (compress) {

        if (USE_SNAPPY) {
            std::string snappy_out;
            comprlen = snappy::Compress(src->data(), (size_t) src->size(), &snappy_out);
            if (comprlen > 0) {
                dst->append(replic_save_len(comprlen));
                dst->append(snappy_out.data(), (int) comprlen);
            }
        } else {
            std::unique_ptr<void, cfree_delete<void>> out(malloc(outlen + 1));
            comprlen = lzf_compress(src->data(), (unsigned int) src->size(), out.get(), outlen);
            if (comprlen > 0) {
                dst->append(replic_save_len(comprlen));
                dst->append(out.get(), (int) comprlen);
            }
        }


    } else {
        comprlen = 0;
    }
#else
    comprlen = 0;
#endif


    if (comprlen == 0) {
        dst->append(replic_save_len(comprlen));
        dst->append(src->data(), src->size());
    }

    src->decr(src->size());
    src->nice();
}

void moveBufferAsync(ReplicationByIterator2 *job, Buffer *dst, Buffer *input, bool compress) {
    if (!compress && input != nullptr) {
        return moveBufferSync(dst, input, false);
    }

    if (job->bg.valid()) {

        PTST(WAIT_CompressResult, 0.005)
        const CompressResult &buf = job->bg.get();
        PTE(WAIT_CompressResult, "")

        auto in = buf.in;
        auto comprlen = buf.comprlen;
        auto rawlen = buf.rawlen;

        saveStrToBuffer(dst, "mset");
        dst->append(replic_save_len((uint64_t) rawlen));
        dst->append(replic_save_len(comprlen));

        if (comprlen == 0) {
            dst->append(in->data(), in->size());
            in->reset();
        } else {
            dst->append(buf.out.data(), (int) comprlen);
        }

//        delete buf.in;
    }


    if (input != nullptr) {
//        auto copy = new Buffer(input->size());
//        copy->append(input->data(), input->size());
//        input->decr(input->size());
//        input->nice();

//        auto copy = new Buffer(input->size());
        swap(job->buffer2, input);

        job->bg = std::async(std::launch::async, [](Buffer *b) {
            CompressResult compressResult;
            compressResult.in = b;
            compressResult.rawlen = (size_t) b->size();

            auto src = compressResult.in;

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

            if (USE_SNAPPY) {
                comprlen = snappy::Compress(src->data(), (size_t) src->size(), &compressResult.out);
                if (comprlen > 0) {

                }
            } else {
                std::unique_ptr<void, cfree_delete<void>> out(malloc(outlen + 1));
                comprlen = lzf_compress(src->data(), (unsigned int) src->size(), out.get(), outlen);
                if (comprlen > 0) {
                    compressResult.out.append((char *) out.get(), (int) comprlen);
                }
            }

            compressResult.comprlen = comprlen;
            if (comprlen != 0) {
                b->reset();
            }

            return compressResult;
        }, job->buffer2);
    }

}