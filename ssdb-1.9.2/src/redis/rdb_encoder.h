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

extern "C" {
#include "crc64.h"
#include "endianconv.h"
};

class RdbEncoder {
public:
    void encodeHeader() {

    }


    void write(const char *s, size_t n) {
        w.append(s, n);
    }

    int rdbWriteRaw(void *p, size_t n) {
        w.append((const char *) p, n);
        return n;
    }

    int rdbSaveLen(uint64_t len);

    int encodeString(const std::string &string);

    int saveRawString(const std::string &string);

    int saveDoubleValue(double value);

    void rdbSaveType(unsigned char type);


    int rdbSaveBinaryDoubleValue(double val);

    int rdbSaveBinaryFloatValue(float val);

    int rdbTryIntegerEncoding(const std::string &string, unsigned char *enc) {
        int64_t value = str_to_int64(string);
        if (errno != 0) {
            return 0;
        }

        std::string newValue = str(value);

        if (newValue.length() != string.length() || newValue != string) return 0;

        return rdbEncodeInteger(value, enc);
    }

    int rdbEncodeInteger(long long value, unsigned char *enc) {
        if (value >= -(1<<7) && value <= (1<<7)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
            enc[1] = value&0xFF;
            return 2;
        } else if (value >= -(1<<15) && value <= (1<<15)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
            enc[1] = value&0xFF;
            enc[2] = (value>>8)&0xFF;
            return 3;
        } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
            enc[1] = value&0xFF;
            enc[2] = (value>>8)&0xFF;
            enc[3] = (value>>16)&0xFF;
            enc[4] = (value>>24)&0xFF;
            return 5;
        } else {
            return 0;
        }
    }

    void encodeFooter();

    std::string toString() const;

private:
    std::string w;


};


#endif //SSDB_RDB_ENCODER_H
