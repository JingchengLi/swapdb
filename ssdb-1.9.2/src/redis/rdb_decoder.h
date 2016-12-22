//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_DECODER_H
#define SSDB_RDB_DECODER_H

#include <cstring>
#include "string"
#include "rdb.h"
#include "endianconv.h"
#include "crc64.h"

class RdbDecoder {
private:
    std::string raw;
    char type;

public:
    RdbDecoder(const std::string &raw);

    bool verifyDumpPayload() {
        const char *footer;
        uint16_t rdbver;
        uint64_t crc;

        /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
        if (raw.length() < 10) return false;
        footer = raw.data() + (raw.length() - 10);

        /* Verify RDB version */
        rdbver = (footer[1] << 8) | footer[0];
        if (rdbver > RDB_VERSION) return false;

        /* Verify CRC64 */
        crc = crc64(0, (const unsigned char *) raw.data(), raw.length() - 8);
        memrev64ifbe(&crc);
        return (std::memcmp(&crc, footer + 2, 8) == 0) ? true : false;
    }

    int rdbLoadObjectType() {
        int type;
        if ((type = raw[0]) == -1) return -1;
        if (!rdbIsObjectType(type)) return -1;
        return type;
    }


};

#endif //SSDB_RDB_DECODER_H
