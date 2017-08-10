/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_REPLICATION_H
#define SSDB_REPLICATION_H


#include <include.h>
#include <string>
#include <sstream>
#include <util/strings.h>
#include <util/thread.h>
#include <net/redis/redis_client.h>
#include <common/context.hpp>
#include <net/worker.h>
#include <thread>
#include <future>

class Fdevents;

#ifdef USE_LEVELDB
namespace leveldb{
#else
#define leveldb rocksdb
namespace rocksdb {
#endif
    class Slice;

}


const uint64_t MAX_PACKAGE_SIZE = 8 * 1024 * 1024;
const uint64_t MIN_PACKAGE_SIZE = 1024 * 1024;

//#define REPLIC_NO_COMPRESS TRUE


void *ssdb_sync2(void *arg);

int replic_decode_len(const char *data, int *offset, uint64_t *lenptr);
std::string replic_save_len(uint64_t len);

void send_error_to_redis(Link *link);
void saveStrToBuffer(Buffer *buffer, const Bytes &fit);


class CompressResult {
public:
    Buffer* in = nullptr;
    std::string out;
    size_t comprlen = 0;
    size_t rawlen = 0;
};


#define USE_SNAPPY true


class ReplicationByIterator2 : public BackgroundThreadJob {
public:
    int64_t ts;

    HostAndPort hnp;

    bool compress = true;
    bool heartbeat = false;

    volatile bool quit = false;

    void reportError();

    int callback(NetworkServer *nets, Fdevents *fdes) override;


    ReplicationByIterator2(const Context &ctx, const HostAndPort &hnp, Link *link, bool compress, bool heartbeat, int64_t t) :
            hnp(hnp), compress(compress), heartbeat(heartbeat) {
        buffer = new Buffer(MAX_PACKAGE_SIZE);
        buffer2 = new Buffer(MAX_PACKAGE_SIZE);

        for (int i = 0; i < quickmap_size; ++i) {
            quickmap.emplace_back(replic_save_len((uint64_t) i));
        }
        replTs = t;

        ts = time_ms();
        BackgroundThreadJob::ctx = ctx;
        BackgroundThreadJob::client_link = link;
    }

    ~ReplicationByIterator2() override {
        if (bg.valid()) {
            bg.get(); //wait for end
        }
        delete buffer;
        delete buffer2;
    };
    int process() override;

    std::future<CompressResult> bg;

    int64_t replTs = 0;


    Buffer *buffer = nullptr;
    Buffer *buffer2 = nullptr;

    inline void saveStrToBufferQuick(Buffer *buffer, const Bytes &fit);
    inline void saveStrToBufferQuick(Buffer *buffer, const leveldb::Slice &fit);


    int quickmap_size = 1024;
    std::vector<std::string> quickmap;

};


#endif //SSDB_REPLICATION_H
