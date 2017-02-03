//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <sstream>
#include <string>
#include <netinet/in.h>
#include <util/strings.h>
#include "rdb.h"

class RdbEncoder {
public:
    void encodeHeader() {

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


};


#endif //SSDB_RDB_ENCODER_H
