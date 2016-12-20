//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_ENCODER_H
#define SSDB_RDB_ENCODER_H

#include <sstream>
#include <string>
#include "rdb.h"
#include "crc64.h"
#include "endianconv.h"

class RdbEncoder {
public:
    void encodeHeader() {

    }


    void encodeType(unsigned char type) {
        w.append((const char *) type, 1);
    }

    void encodeFooter() {

        unsigned char buf[2];
        buf[0] = RDB_VERSION & 0xff;
        buf[1] = (RDB_VERSION >> 8) & 0xff;

        w.append((const char *) buf, 2);

        uint64_t crc;
        crc = crc64(0, (unsigned char *) w.data(), w.length());
        memrev64ifbe(&crc);

        w.append((const char *) crc, 8);
    }


private:
    std::string w;


};


#endif //SSDB_RDB_ENCODER_H
