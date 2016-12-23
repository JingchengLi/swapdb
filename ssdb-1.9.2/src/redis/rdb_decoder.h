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


    int rdbLoadDoubleValue(double *val) {
        char buf[256];
        unsigned char len;

        if (rioRead(&len,1) == 0) return -1;
        switch(len) {
            case 255: *val = R_NegInf; return 0;
            case 254: *val = R_PosInf; return 0;
            case 253: *val = R_Nan; return 0;
            default:
                if (rioRead(buf,len) == 0) return -1;
                buf[len] = '\0';
                sscanf(buf, "%lg", val);
                return 0;
        }
    }

    int rdbLoadBinaryDoubleValue(double *val) {
        if (rioRead(val,sizeof(*val)) == 0) return -1;
        memrev64ifbe(val);
        return 0;
    }


};

#endif //SSDB_RDB_DECODER_H
