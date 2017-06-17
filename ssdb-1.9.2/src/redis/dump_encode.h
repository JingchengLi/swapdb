//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <string>
#include "rdb.h"
#include "redis_encoder.h"


extern "C" {
#include "crc64.h"
#include "endianconv.h"
};



class DumpEncoder : public RedisEncoder {
public:

    std::string w;

    explicit DumpEncoder(bool rdb_compression) {
        RedisEncoder::rdb_compression = rdb_compression;
        w.reserve(1024); //1k
    }

    DumpEncoder() {
        w.reserve(1024); //1k
    }

    std::string toString() const {
        return w;
    }


    int rdbWriteRaw(void *p, size_t n) {
        w.append((const char *) p, n);
        return (int) n;
    }


    void encodeFooter() {

        unsigned char buf[2];
        buf[0] = RDB_VERSION & 0xff;
        buf[1] = (RDB_VERSION >> 8) & 0xff;
        rdbWriteRaw(&buf, 2);

        uint64_t crc;
        crc = crc64_fast(0, w.data(), w.size());
        memrev64ifbe(&crc);
        rdbWriteRaw(&crc, 8);

    }


};


#endif //SSDB_RDB_ENCODER_H
