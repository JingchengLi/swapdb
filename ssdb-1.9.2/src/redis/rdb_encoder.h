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
#include "lzf.h"
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

    int rdbSaveRawString(const std::string &string);

    ssize_t rdbSaveLzfBlob(void *data, size_t compress_len, size_t original_len) {
        unsigned char byte;
        ssize_t n, nwritten = 0;

        /* Data compressed! Let's save it on disk */
        byte = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
        if ((n = rdbWriteRaw(&byte, 1)) == -1) goto writeerr;
        nwritten += n;

        if ((n = rdbSaveLen(compress_len)) == -1) goto writeerr;
        nwritten += n;

        if ((n = rdbSaveLen(original_len)) == -1) goto writeerr;
        nwritten += n;

        if ((n = rdbWriteRaw(data, compress_len)) == -1) goto writeerr;
        nwritten += n;

        return nwritten;

        writeerr:
        return -1;
    }

    int64_t rdbSaveLzfStringObject(const std::string &string) {
        size_t len = string.length();
        size_t comprlen, outlen;
        void *out;

        /* We require at least four bytes compression for this to be worth it */
        if (len <= 4) return 0;
        outlen = len - 4;
        if ((out = malloc(outlen + 1)) == NULL) return 0;
        comprlen = lzf_compress(string.data(), len, out, outlen);
        if (comprlen == 0) {
            free(out);
            return 0;
        }

        ssize_t nwritten = rdbSaveLzfBlob(out, comprlen, len);
        free(out);
        return nwritten;
    }


    int saveRawString(const std::string &string);

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
