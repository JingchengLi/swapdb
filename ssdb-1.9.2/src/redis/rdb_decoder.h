/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_RDB_DECODER_H
#define SSDB_RDB_DECODER_H

#include <cstring>
#include "string"
#include "rdb.h"
extern "C" {
#include "crc64.h"
};


const double R_Zero = 0.0;
const double R_PosInf = 1.0/R_Zero;
const double R_NegInf = -1.0/R_Zero;
const double R_Nan = R_Zero/R_Zero;


class RdbDecoder {
private:

    const char *p = nullptr;  //pointer to current first char
    size_t remain_size = 0;       // string remain len

    RdbDecoder() = default;

public:
    virtual ~RdbDecoder() = default;

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
