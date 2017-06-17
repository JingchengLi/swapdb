//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <string>
#include "rdb.h"
#include "util/bytes.h"
#include "redis_encoder.h"

class dump_encoder : public RedisEncoder {
public:

    std::string w;

    dump_encoder(bool rdb_compression) {
        RedisEncoder::rdb_compression = rdb_compression;
        w.reserve(1024); //1k
    }

    dump_encoder() {
        w.reserve(1024); //1k
    }

    std::string toString() const {
        return w;
    }


    int rdbWriteRaw(void *p, size_t n) {
        w.append((const char *) p, n);
        return n;
    }


    char *data() override {
        return (char *) w.data();
    }

    size_t size() override {
        return w.size();
    }


};


#endif //SSDB_RDB_ENCODER_H
