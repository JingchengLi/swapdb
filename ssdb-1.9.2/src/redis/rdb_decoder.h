//
// Created by zts on 16-12-20.
//

#ifndef SSDB_RDB_DECODER_H
#define SSDB_RDB_DECODER_H

#include <cstring>
#include <util/bytes.h>
#include "string"
#include "rdb.h"
#include <netinet/in.h>
#include <util/log.h>


extern "C" {
#include "lzf.h"
#include "crc64.h"
#include "endianconv.h"
};


#define rdbExitReportCorruptRDB(...) exit(-1)

class RdbDecoder {
private:

    const char *p;  //pointer to current first char
    size_t remain_size;       // string remain len

    std::string raw;
    char type;

    RdbDecoder() {};

public:
    virtual ~RdbDecoder() {
    }

    RdbDecoder(const std::string &raw) : raw(raw) {
        p = raw.data();
        remain_size = raw.length();
    }

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

        remain_size = remain_size - 10;
        return std::memcmp(&crc, footer + 2, 8) == 0;
    }

    int rdbLoadType() {
        unsigned char type;
        if (rioRead(&type, 1) == 0) return -1;
        return type;
    }

//    int rdbLoadObjectType() {
//
//        int type;
//        if ((type = raw[0]) == -1) return -1;
//        if (!rdbIsObjectType(type)) return -1;
//
//        p = p + 1;
//        remain_size = remain_size - 1;
//
//        return type;
//    }

    int rdbLoadObjectType() {

        int type;
        if ((type = rdbLoadType()) == -1) return -1;
        if (!rdbIsObjectType(type)) return -1;

//        p = p + 1;
//        remain_size = remain_size - 1;

        return type;
    }

    std::string rdbGenericLoadStringObject(int *ret) {
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
                default:
                    rdbExitReportCorruptRDB("Unknown RDB string encoding type %d", len);
            }
        }

        if (len == RDB_LENERR) {
            *ret = -1;
            return "";
        };

        char *buf = (char *) malloc(len);
        if (len && rioRead(buf, len) == 0) {
            *ret = -1;
            return "";
        } else {
            std::string tmp(buf, len);
            free(buf);
            *ret = 0;
            return tmp;
        }
    }

    uint64_t rdbLoadLen(int *isencoded) {
        uint64_t len;
        if (rdbLoadLenByRef(isencoded, &len) == -1) return RDB_LENERR;
        return len;
    }

    int rdbLoadLenByRef(int *isencoded, uint64_t *lenptr);

    inline size_t rioRead(void *buf, size_t len);


    std::string rdbLoadIntegerObject(int enctype, int *ret) {

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

    std::string rdbLoadLzfStringObject(int *ret) {
        uint64_t len, clen;
        unsigned char *c = NULL;
        char *t_val = NULL;

        if ((clen = rdbLoadLen(NULL)) == RDB_LENERR) return NULL;
        if ((len = rdbLoadLen(NULL)) == RDB_LENERR) return NULL;
        if ((c = (unsigned char *) malloc(clen)) == NULL) {
            free(c);
            *ret = -1;
            return "";
        }

        /* Allocate our target according to the uncompressed size. */
        t_val = (char *) malloc(len);

        /* Load the compressed representation and uncompress it to target. */
        if (rioRead(c, clen) == 0) {
            free(c);
            free(t_val);
            *ret = -1;
            return "";
        }

        if (lzf_decompress(c, clen, t_val, len) == 0) {
            log_error("Invalid LZF compressed string %s %s", strerror(errno) , hexmem(c,clen).c_str());
            free(c);
            free(t_val);
            *ret = -1;
            return "";
        }

        free(c);
        std::string tmp(t_val, len);
        free(t_val);
        *ret = 0;
        return tmp;
    }
};

#endif //SSDB_RDB_DECODER_H
