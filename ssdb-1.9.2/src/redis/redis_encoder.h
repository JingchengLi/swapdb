/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_REDISENCODER_H
#define SSDB_REDISENCODER_H


#include "util/bytes.h"

class RedisEncoder {

protected:
    bool rdb_compression = false;
public:

    virtual int rdbWriteRaw(void *p, size_t n) = 0;

    int rdbSaveLen(uint64_t len);

    int rdbSaveType(unsigned char type);

    int rdbSaveMillisecondTime(long long t);

    int rdbSaveAuxField(const std::string &key, const std::string &val);

    int rdbSaveAuxFieldStrStr(const std::string &key, const std::string &val);

    int rdbSaveAuxFieldStrInt(const std::string &key, long long val);

    int rdbSaveObjectType(char dtype);

    int64_t rdbSaveRawString(const std::string &string);

    int64_t saveRawString(const std::string &string);

    int64_t saveRawString(const Bytes &string);

    int saveDoubleValue(double value);

    int rdbSaveBinaryDoubleValue(double val);

    int rdbSaveBinaryFloatValue(float val);

    int rdbEncodeInteger(long long value, unsigned char *enc);

    int rdbTryIntegerEncoding(const std::string &string, unsigned char *enc);

    int64_t rdbSaveLzfStringObject(const std::string &string);

    int64_t rdbSaveLzfBlob(void *data, size_t compress_len, size_t original_len);
};


#endif //SSDB_REDISENCODER_H
