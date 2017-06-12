//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <string>
#include "rdb.h"
#include "util/bytes.h"

class RdbEncoder {
public:

    RdbEncoder(bool rdb_compression) : rdb_compression(rdb_compression) {
        w.reserve(1024); //1k
    }

    RdbEncoder() {
        w.reserve(1024); //1k
    }

    int rdbWriteRaw(void *p, size_t n) {
        w.append((const char *) p, n);
        return n;
    }

    int rdbSaveLen(uint64_t len);

    int64_t rdbSaveRawString(const std::string &string);

    int64_t rdbSaveLzfBlob(void *data, size_t compress_len, size_t original_len);

    int64_t rdbSaveLzfStringObject(const std::string &string);

    int64_t saveRawString(const std::string &string);

    int64_t saveRawString(const Bytes &string);

    int saveDoubleValue(double value);

    void rdbSaveType(unsigned char type);

    int rdbSaveBinaryDoubleValue(double val);

    int rdbSaveBinaryFloatValue(float val);

    int rdbTryIntegerEncoding(const std::string &string, unsigned char *enc);

    int rdbEncodeInteger(long long value, unsigned char *enc);

    void encodeFooter();

    std::string toString() const;

private:
    std::string w;

    bool rdb_compression = false;
};


#endif //SSDB_RDB_ENCODER_H
