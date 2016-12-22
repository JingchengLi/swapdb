//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_DECODER_H
#define SSDB_RDB_DECODER_H

#include <cstring>
#include <util/bytes.h>
#include "string"
#include "rdb.h"
#include <netinet/in.h>
#include <util/log.h>


extern "C" {
#include "lzf.h"
#include "crc64.h"
#include "endianconv.h"
};

//TODO rdbExitReportCorruptRDB
#define rdbExitReportCorruptRDB(...) exit(-1)

class RdbDecoder {
private:

    const char *p;  //pointer to current first char
    size_t remain_size;       // string remain len

    std::string raw;
    char type;

    RdbDecoder() {};

public:
    virtual ~RdbDecoder() {
    }

    RdbDecoder(const std::string &raw) : raw(raw) {
        p = raw.data();
        remain_size = raw.length();
    }

    bool verifyDumpPayload();

    int rdbLoadType();

    int rdbLoadObjectType();

    std::string rdbGenericLoadStringObject(int *ret);

    uint64_t rdbLoadLen(int *isencoded);

    int rdbLoadLenByRef(int *isencoded, uint64_t *lenptr);

    inline size_t rioRead(void *buf, size_t len);


    std::string rdbLoadIntegerObject(int enctype, int *ret);

    std::string rdbLoadLzfStringObject(int *ret);
};

#endif //SSDB_RDB_DECODER_H
