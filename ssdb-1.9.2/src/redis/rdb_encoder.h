//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <sstream>
#include <string>
#include <netinet/in.h>
#include "rdb.h"
#include "endianconv.h"

extern "C" {
#include "crc64.h"
};

class RdbEncoder {
public:
    void encodeHeader() {

    }


    void write(const char *s, size_t n) {
        w.append(s, n);
    }

    int writeRaw(void *p, size_t n) {
        w.append((const char *) p, n);
        return 0;
    }

    int saveLen(uint32_t len);

    int encodeString(const std::string &string);

    int saveRawString(const std::string &string);

    int saveDoubleValue(double value);

    int encodeIntString(const std::string &string);

    void saveType(unsigned char type);

    void encodeFooter();


    std::string toString() const;

private:
    std::string w;


};


#endif //SSDB_RDB_ENCODER_H
