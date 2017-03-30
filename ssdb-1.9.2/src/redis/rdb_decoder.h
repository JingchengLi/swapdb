//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_DECODER_H
#define SSDB_RDB_DECODER_H

#include <cstring>
#include "string"
#include "rdb.h"


class RdbDecoder {
private:

    const char *p;  //pointer to current first char
    size_t remain_size;       // string remain len

    char type;

    RdbDecoder() {};

public:
    virtual ~RdbDecoder() {
    }

    RdbDecoder(const char *str, size_t size) {
        p = str;
        remain_size = size;
    }

    bool verifyDumpPayload();

    int rdbLoadType();

    int rdbLoadObjectType();

    std::string rdbGenericLoadStringObject(int *ret);

    uint64_t rdbLoadLen(int *isencoded);

    int rdbLoadLenByRef(int *isencoded, uint64_t *lenptr);

    size_t rioRead(void *buf, size_t len);

    size_t rioReadString(std::string &res, size_t len);

    size_t rioReadString(const char **start , size_t len);

    std::string rdbLoadIntegerObject(int enctype, int *ret);

    std::string rdbLoadLzfStringObject(int *ret);


    int rdbLoadDoubleValue(double *val);

    int rdbLoadBinaryDoubleValue(double *val);


};

#endif //SSDB_RDB_DECODER_H
