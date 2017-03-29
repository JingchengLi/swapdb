//
// Created by zts on 16-12-20.
//

#include "rdb_decoder.h"
#include "util/bytes.h"
#include <netinet/in.h>

extern "C" {
#include "lzf.h"
#include "crc64.h"
#include "endianconv.h"
#include "util.h"
#include "zmalloc.h"
};


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

size_t RdbDecoder::rioReadString(std::string &res, size_t len) {
    if (len > remain_size) {
        return 0;
    }

    res = std::string(p, len);

    p = p + len;
    remain_size = remain_size - len;
    return len;
}

std::string RdbDecoder::rdbLoadIntegerObject(int enctype, int *ret) {

    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(enc, 1) == 0) return NULL;
        val = (signed char) enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(enc, 2) == 0) return NULL;
        v = enc[0] | (enc[1] << 8);
        val = (int16_t) v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(enc, 4) == 0) return NULL;
        v = enc[0] | (enc[1] << 8) | (enc[2] << 16) | (enc[3] << 24);
        val = (int32_t) v;
    } else {
        val = 0; /* anti-warning */
        rdbExitReportCorruptRDB("Unknown RDB integer encoding type %d", enctype);
    }

    return str((int64_t) val);
}

std::string RdbDecoder::rdbLoadLzfStringObject(int *ret) {
    uint64_t len, clen;
    char *t_val = NULL;

    if ((clen = rdbLoadLen(NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(NULL)) == RDB_LENERR) return NULL;


    /* Allocate our target according to the uncompressed size. */

    /* Load the compressed representation and uncompress it to target. */
    std::string tmp_c;
    if (rioReadString(tmp_c, clen) == 0) {
        *ret = -1;
        return "";
    }


    t_val = (char *) zmalloc(len);
    if (lzf_decompress(tmp_c.data(), clen, t_val, len) == 0) {
        free(t_val);
        *ret = -1;
        return "";
    }

    std::string tmp(t_val, len);
    zfree(t_val);
    *ret = 0;
    return tmp;
}

std::string RdbDecoder::rdbGenericLoadStringObject(int *ret) {
    int isencoded;
    uint64_t len;

    len = rdbLoadLen(&isencoded);
    if (isencoded) {
        switch (len) {
            case RDB_ENC_INT8:
            case RDB_ENC_INT16:
            case RDB_ENC_INT32:
                return rdbLoadIntegerObject(len, ret);
            case RDB_ENC_LZF:
                return rdbLoadLzfStringObject(ret);
            default: rdbExitReportCorruptRDB("Unknown RDB string encoding type %d", len);
        }
    }

    if (len == RDB_LENERR) {
        *ret = -1;
        return "";
    }

    std::string tmp;
    if (len && rioReadString(tmp, len) == 0) {
        *ret = -1;
        return "";
    } else {
        *ret = 0;
        return tmp;
   }

}

bool RdbDecoder::verifyDumpPayload() {
    const char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    if (remain_size < 10) return false;
    footer = p + (remain_size - 10);

    /* Verify RDB version */
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver > RDB_VERSION) return false;

    /* Verify CRC64 */
    crc = crc64(0, (const unsigned char *) p, remain_size - 8);
    memrev64ifbe(&crc);

    remain_size = remain_size - 10;
    return std::memcmp(&crc, footer + 2, 8) == 0;
}

int RdbDecoder::rdbLoadObjectType() {
    int type;
    if ((type = rdbLoadType()) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

uint64_t RdbDecoder::rdbLoadLen(int *isencoded) {
    uint64_t len;
    if (rdbLoadLenByRef(isencoded, &len) == -1) return RDB_LENERR;
    return len;
}

int RdbDecoder::rdbLoadType() {
    unsigned char type;
    if (rioRead(&type, 1) == 0) return -1;
    return type;
}

int RdbDecoder::rdbLoadBinaryDoubleValue(double *val) {
    if (rioRead(val,sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
}

int RdbDecoder::rdbLoadDoubleValue(double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(&len,1) == 0) return -1;
    switch(len) {
        case 255: *val = R_NegInf; return 0;
        case 254: *val = R_PosInf; return 0;
        case 253: *val = R_Nan; return 0;
        default:
            if (rioRead(buf,len) == 0) return -1;
            buf[len] = '\0';
            sscanf(buf, "%lg", val);
            return 0;
    }
}
