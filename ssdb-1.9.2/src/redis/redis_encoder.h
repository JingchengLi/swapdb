//
// Created by zts on 17-6-17.
//

#ifndef SSDB_REDISENCODER_H
#define SSDB_REDISENCODER_H


#include "util/bytes.h"

class RedisEncoder {

protected:
    bool rdb_compression = false;
public:
    void encodeFooter();

    virtual int rdbWriteRaw(void *p, size_t n) = 0;

    virtual char *data() = 0;

    virtual size_t size() = 0;


    int rdbSaveLen(uint64_t len);

    void rdbSaveType(unsigned char type);

    int64_t rdbSaveRawString(const std::__cxx11::string &string);

    int64_t saveRawString(const std::__cxx11::string &string);

    int64_t saveRawString(const Bytes &string);

    int saveDoubleValue(double value);

    int rdbSaveBinaryDoubleValue(double val);

    int rdbSaveBinaryFloatValue(float val);

    int rdbEncodeInteger(long long value, unsigned char *enc);

    int rdbTryIntegerEncoding(const std::__cxx11::string &string, unsigned char *enc);

    int64_t rdbSaveLzfStringObject(const std::__cxx11::string &string);

    int64_t rdbSaveLzfBlob(void *data, size_t compress_len, size_t original_len);
};


#endif //SSDB_REDISENCODER_H
