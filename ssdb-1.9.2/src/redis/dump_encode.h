/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

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


    int encodeFooter() {

        unsigned char buf[2];
        buf[0] = RDB_VERSION & 0xff;
        buf[1] = (RDB_VERSION >> 8) & 0xff;

        if (rdbWriteRaw(&buf, 2) == -1) return -1;

        uint64_t crc;
        crc = crc64_fast(0, w.data(), w.size());
        memrev64ifbe(&crc);

        if (rdbWriteRaw(&crc, 8) == -1) return -1;

        return 0;
    }


};


#endif //SSDB_RDB_ENCODER_H
