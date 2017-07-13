//
// Created by zts on 17-5-10.
//


#include "replication.h"
#include "serv.h"
#ifdef USE_SNAPPY

#include <snappy.h>
#else
#include "util/cfree.h"
extern "C" {
#include <redis/lzf.h>
}
#endif


void *ssdb_sync2(void *arg) {

    ReplicationByIterator *job = (ReplicationByIterator *) arg;
    Context ctx = job->ctx;
    SSDBServer *serv = (SSDBServer *) ctx.net->data;
    HostAndPort hnp = job->hnp;
    std::unique_ptr<Link> master_link(job->client_link); //upstream cannot be null !
    bool heartbeat = job->heartbeat;
    bool quit = job->quit;

    delete job;
    job = nullptr;

    size_t total_threads = 5;
    size_t current_thread = 0;

    auto get_thread_id = [&current_thread, total_threads]() {
        ++current_thread;
        current_thread = (current_thread) % total_threads;
        return current_thread;
    };

    std::vector<std::future<int>> bgs(total_threads);
//    std::future<int> bg;

    log_warn("[ssdb_sync] update transfer state");
    {
        Locking<Mutex> l(&serv->replicState.rMutex);

        if (serv->replicState.inTransState()) {
            log_fatal("i am in transferring state, should not into this step!!!!!!");
        }

        serv->replicState.startReplic();
    }

    log_warn("[ssdb_sync] expiration stop");
    if (serv->ssdb->expiration) {
        serv->ssdb->expiration->stop();
    }

    log_warn("[ssdb_sync] ssdb stop");
    serv->ssdb->stop();

    log_warn("[ssdb_sync] do flushdb");
    serv->ssdb->flushdb(ctx);
    serv->ssdb->resetRepopid(ctx);


    log_warn("[ssdb_sync] ready to revieve");
    master_link->quick_send({"ok", "ready to revieve"});


    log_debug("[ssdb_sync] prepare for event loop");

    unique_ptr<Fdevents> fdes = unique_ptr<Fdevents>(new Fdevents());

    fdes->set(master_link->fd(), FDEVENT_IN, 1, master_link.get()); //open evin
    master_link->noblock(true);

    std::string upstream_ip = serv->opt.upstream_ip;
    int upstream_port = serv->opt.upstream_port;

    RedisUpstream redisUpstream(upstream_ip, upstream_port, 500);

    if (heartbeat) {
        redisUpstream.setMaxRetry(1);
        redisUpstream.setRetryConnect(-1);
        redisUpstream.reset();
        if (!redisUpstream.isConnected()) {
            log_warn("cannot connect to redis");
        }

    }


    const Fdevents::events_t *events;
    ready_list_t ready_list;
    ready_list_t ready_list_2;
    ready_list_t::iterator it;

    int errorCode = 0;


    int64_t lastHeartBeat = time_ms();
    while (!quit) {
        ready_list.swap(ready_list_2);
        ready_list_2.clear();


        if (heartbeat) {
            if ((time_ms() - lastHeartBeat) > 3000) {

                std::unique_ptr<RedisResponse> t_res(
                        redisUpstream.sendCommand({"ssdb-notify-redis", "transfer", "continue"}));
                if (!t_res) {
                    log_warn("send transfer continue to redis<%s:%d> failed", upstream_ip.c_str(), upstream_port);
                }

                lastHeartBeat = time_ms();

            }
        }


        if (!ready_list.empty()) {
            events = fdes->wait(0);
        } else {
            events = fdes->wait(50);
        }

        if (events == nullptr) {
            log_fatal("events.wait error: %s", strerror(errno));
            //exit
            errorCode = -1;
            break;
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
                int len = link->read(MAX_PACKAGE_SIZE);
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

                if (link->error()) {
                    continue;
                }
                int len = link->write();
                if (len <= 0) {
//                if (len < 0) {
                    log_debug("fd: %d, write: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
                    link->mark_error();
                    continue;
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
                //TODO
                errorCode = -2;
                break;
            }

            while (link->input->size() > 1) {
                Decoder decoder(link->input->data(), link->input->size());

                int oper_offset = 0, raw_size_offset = 0, compressed_offset = 0;
                uint64_t oper_len = 0, raw_len = 0, compressed_len = 0;

                if (replic_decode_len(decoder.data(), &oper_offset, &oper_len) == -1) {
                    log_error("replic_decode_len error :  oper_offset %d, oper_len %d", oper_offset, oper_len);
                    errorCode = -3;
                    break;
                }

                if (decoder.skip(oper_offset) == -1) {
                    log_info("skip oper_offset len need retry");
                    link->input->grow();
                    break;
                }

                if (decoder.size() < ((int) oper_len)) {
                    log_info("skip oper_len len need retry");
                    link->input->grow();
                    break;
                }

                std::string oper(decoder.data(), oper_len);
                decoder.skip((int) oper_len);

                if (oper == "mset") {

                    if (decoder.size() < 1) {
                        link->input->grow();
                        break;
                    }
                    if (replic_decode_len(decoder.data(), &raw_size_offset, &raw_len) == -1) {
                        errorCode = -3;
                        break;
                    }
                    decoder.skip(raw_size_offset);

                    if (decoder.size() < 1) {
                        link->input->grow();
                        break;
                    }
                    if (replic_decode_len(decoder.data(), &compressed_offset, &compressed_len) == -1) {
                        errorCode = -3;
                        break;
                    }
                    decoder.skip(compressed_offset);

                    if (decoder.size() < (compressed_len)) {
                        link->input->grow();
                        break;
                    }


                    std::string tmp;
                    std::vector<Bytes> kvs;

                    if (compressed_len == 0) {
                        if (decoder.size() < (raw_len)) {
                            link->input->grow();
                            break;
                        }

                        tmp = std::string(decoder.data(), raw_len);
                        compressed_len = raw_len;
                    } else {

#ifdef USE_SNAPPY
                        if (!snappy::Uncompress(decoder.data(), compressed_len, &tmp)) {
                            errorCode = -4;
                            break;;
                        } else {
                            raw_len = tmp.size();
                        }

#else

                        std::unique_ptr<char, cfree_delete<char>> t_val((char *) malloc(raw_len));

                        if (lzf_decompress(decoder.data(), compressed_len, t_val.get(), raw_len) == 0) {
                            errorCode = -4;
                            break;;
                        }
                        tmp = std::move(std::string(t_val.get(), raw_len));
#endif
                    }

                    Decoder decoder_item(tmp.data(), raw_len);

                    uint64_t remian_length = raw_len;
                    while (remian_length > 0) {
                        int key_offset = 0, val_offset = 0;
                        uint64_t key_len = 0, val_len = 0;

                        if (replic_decode_len(decoder_item.data(), &key_offset, &key_len) == -1) {
                            errorCode = -3;
                            break;
                        }
                        decoder_item.skip(key_offset);

//                        std::string key(decoder_item.data(), key_len);
                        const char *key_p = decoder_item.data();

                        decoder_item.skip((int) key_len);
                        remian_length -= (key_offset + (int) key_len);

                        if (replic_decode_len(decoder_item.data(), &val_offset, &val_len) == -1) {
                            errorCode = -3;
                            break;
                        }
                        decoder_item.skip(val_offset);

//                        std::string value(decoder_item.data(), val_len);
                        const char *value_p = decoder_item.data();

                        decoder_item.skip((int) val_len);
                        remian_length -= (val_offset + (int) val_len);
//
//                        kvs.emplace_back(key);
//                        kvs.emplace_back(value);

                        kvs.emplace_back(key_p, key_len);
                        kvs.emplace_back(value_p, val_len);


                    }

                    decoder.skip(compressed_len);
                    link->input->decr(link->input->size() - decoder.size());

                    if (!kvs.empty()) {

                        int tid = get_thread_id();
                        if (bgs[tid].valid()) {

                            PTST(WAIT_parse_replic, 0.05)
                            errorCode = bgs[tid].get();
                            PTE(WAIT_parse_replic, str(tid))
                        }

                        /*
                         * attention here
                         * we use vector<Bytes> instead of vector<string>
                         * using data_hold to keep the memory area reserved
                         * */

                        bgs[tid] = std::async(std::launch::async,
                                              [&serv, &ctx](std::vector<Bytes> arr, std::string data_hold) {
                                                  //serv is thread safe and ctx is readonly
                                                  log_debug("parse_replic count %d , data size %d", arr.size(),
                                                            data_hold.size());

                                                  return serv->ssdb->parse_replic(ctx, arr);
                                              }, std::move(kvs), std::move(tmp));

                    }

                } else if (oper == "complete") {
                    link->input->decr(link->input->size() - decoder.size());
                    quit = true;
                } else {
                    //TODO 处理遇到的 oper_len == 0 ???
                    log_error("unknown oper code %s", hexstr(oper).c_str());
                    errorCode = -5;
                    break;
                }
            }

            if (errorCode != 0) {
                break;
            }
        }

        if (errorCode != 0) {
            break;
        }

    }

    for (int tid = 0; tid < bgs.size(); ++tid) {
        if (bgs[tid].valid()) {
            errorCode = bgs[tid].get();
        }
    }

//    if (!kvs.empty()) {
//        log_error("??????????????????????????????????????????");
//        log_debug("parse_replic count %d", kvs.size());
//        errorCode = serv->ssdb->parse_replic(ctx, kvs);
//        kvs.clear();
//    }

    log_info("[ssdb_sync] flush memtable");
    serv->ssdb->flush(ctx, true);

    log_info("[ssdb_sync] expiration starting");

    if (serv->ssdb->expiration != nullptr) {
        serv->ssdb->expiration->start();
    }

    log_info("[ssdb_sync] ssdb starting");

    serv->ssdb->start();

    if (errorCode != 0) {
        master_link->quick_send({"error", "recieve snapshot failed!"});
        log_error("[ssdb_sync] recieve snapshot from %s failed!, err: %d", hnp.String().c_str(), errorCode);

        if (heartbeat) {
            std::unique_ptr<RedisResponse> t_res(
                    redisUpstream.sendCommand({"ssdb-notify-redis", "transfer", "unfinished"}));
            if (!t_res) {
                log_warn("send transfer unfinished to redis<%s:%d> failed", upstream_ip.c_str(), upstream_port);
            }
        }
    } else {
        master_link->quick_send({"ok", "recieve snapshot finished"});
        log_info("[ssdb_sync] recieve snapshot from %s finished!", hnp.String().c_str());

        if (heartbeat) {
            std::unique_ptr<RedisResponse> t_res(
                    redisUpstream.sendCommand({"ssdb-notify-redis", "transfer", "finished"}));
            if (!t_res) {
                log_warn("send transfer finished to redis<%s:%d> failed", upstream_ip.c_str(), upstream_port);
            }
        }
    }


    {
        Locking<Mutex> l(&serv->replicState.rMutex);
        serv->replicState.finishReplic((errorCode == 0));
    }

    return (void *) NULL;
}

