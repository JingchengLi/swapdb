//
// Created by zts on 16-12-20.
//

#include "rdb_decoder.h"

int RdbDecoder::rdbLoadLenByRef(int *isencoded, uint64_t *lenptr) {
    unsigned char buf[2];
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(buf, 1) == 0) return -1;
    type = (buf[0] & 0xC0) >> 6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        *lenptr = buf[0] & 0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0] & 0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(buf + 1, 1) == 0) return -1;
        *lenptr = ((buf[0] & 0x3F) << 8) | buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        if (rioRead(&len, 4) == 0) return -1;
        *lenptr = ntohl(len);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        if (rioRead(&len, 8) == 0) return -1;
        *lenptr = ntohu64(len);
    } else {
        rdbExitReportCorruptRDB(
                "Unknown length encoding %d in rdbLoadLen()", type);
        return -1; /* Never reached. */
    }
    return 0;
}

size_t RdbDecoder::rioRead(void *buf, size_t len) {

    if (len > remain_size) {
        return 0;
    }

    memcpy((char *) buf, p, len);
    p = p + len;
    remain_size = remain_size - len;
    return len;
}
