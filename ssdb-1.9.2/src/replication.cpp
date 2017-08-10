/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "replication.h"
#include "serv.h"

extern "C" {
#include <redis/rdb.h>
};

void send_error_to_redis(Link *link) {
    if (link != nullptr) {
        link->quick_send({"ok", "rr_transfer_snapshot unfinished"});
        log_error("send rr_transfer_snapshot error!!");
    }
}

std::string replic_save_len(uint64_t len) {
    std::string res;

    unsigned char buf[2];

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        res.append(1, buf[0]);
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        res.append(1, buf[0]);
        res.append(1, buf[1]);
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        res.append(1, buf[0]);
        uint32_t len32 = htobe32(len);
        res.append((char *) &len32, sizeof(uint32_t));
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        res.append(1, buf[0]);
        len = htobe64(len);
        res.append((char *) &len, sizeof(uint64_t));
    }
    return res;
}


int replic_decode_len(const char *data, int *offset, uint64_t *lenptr) {
    unsigned char buf[2];
    buf[0] = (unsigned char) data[0];
    buf[1] = (unsigned char) data[1];
    int type;
    type = (buf[0] & 0xC0) >> 6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        *lenptr = buf[0] & 0x3F;
        *offset = 1;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0] & 0x3F;
        *offset = 1;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        *lenptr = ((buf[0] & 0x3F) << 8) | buf[1];
        *offset = 2;
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        len = *(uint32_t *) (data + 1);
        *lenptr = be32toh(len);
        *offset = 1 + sizeof(uint32_t);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        len = *(uint64_t *) (data + 1);
        *lenptr = be64toh(len);
        *offset = 1 + sizeof(uint64_t);
    } else {
        printf("Unknown length encoding %d in rdbLoadLen()", type);
        return -1; /* Never reached. */
    }
    return 0;
}

void saveStrToBuffer(Buffer *buffer, const Bytes &fit) {
    string val_len = replic_save_len((uint64_t) (fit.size()));
    buffer->append(val_len);
    buffer->append(fit);
}
