//
// Created by zts on 16-12-20.
//

#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <memory>

#include "rdb_encoder.h"
#include "util/strings.h"
#include "util/cfree.h"

extern "C" {
#include "lzf.h"
#include "crc64.h"
#include "endianconv.h"
#include "zmalloc.h"
};


void RdbEncoder::encodeFooter() {

    unsigned char buf[2];
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    rdbWriteRaw(&buf, 2);

    uint64_t crc;
    crc = crc64_fast(0, (unsigned char *) w.data(), w.length());
    memrev64ifbe(&crc);
    rdbWriteRaw(&crc, 8);

}

int RdbEncoder::rdbSaveLen(uint64_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        if (rdbWriteRaw(buf, 1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        if (rdbWriteRaw(buf, 2) == -1) return -1;
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        if (rdbWriteRaw(buf, 1) == -1) return -1;
        uint32_t len32 = htonl(len);
        if (rdbWriteRaw(&len32, 4) == -1) return -1;
        nwritten = 1 + 4;
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        if (rdbWriteRaw(buf, 1) == -1) return -1;
        len = htonu64(len);
        if (rdbWriteRaw(&len, 8) == -1) return -1;
        nwritten = 1 + 8;
    }

    return nwritten;
}

void RdbEncoder::rdbSaveType(unsigned char type) {
    rdbWriteRaw(&type, 1);
}

std::string RdbEncoder::toString() const {
    return w;
}

int64_t RdbEncoder::rdbSaveRawString(const std::string &string) {
    size_t len = string.length();
    if (len <= 11) {
        int enclen;
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding(string, buf)) > 0) {
            if (rdbWriteRaw(buf, enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
    * aaaaaaaaaaaaaaaaaa so skip it */
    if (rdb_compression && len > 20) {
        int64_t n = rdbSaveLzfStringObject(string);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    rdbSaveLen(string.length());
    rdbWriteRaw((void *) string.data(), string.length());

    return string.length();
}


int64_t RdbEncoder::saveRawString(const std::string &string) {

    rdbSaveLen(string.size());
    rdbWriteRaw((void *) string.data(), string.size());

    return string.size();
}



int64_t RdbEncoder::saveRawString(const Bytes &string) {
    uint64_t size = (uint64_t) string.size();

    rdbSaveLen(size);
    rdbWriteRaw((void *) string.data(), size);

    return size;
}


int RdbEncoder::saveDoubleValue(double val) {

    unsigned char buf[128];
    int len;

    if (std::isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!std::isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
        snprintf((char *) buf + 1, sizeof(buf) - 1, "%.17g", val);
        buf[0] = std::strlen((char *) buf + 1);
        len = buf[0] + 1;
    }

    rdbWriteRaw(&buf, len);

    return 1;
}

int RdbEncoder::rdbSaveBinaryDoubleValue(double val) {
    memrev64ifbe(&val);
    return rdbWriteRaw(&val, sizeof(val));
}

int RdbEncoder::rdbSaveBinaryFloatValue(float val) {
    memrev32ifbe(&val);
    return rdbWriteRaw(&val, sizeof(val));
}

int RdbEncoder::rdbEncodeInteger(long long value, unsigned char *enc) {
    if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT8;
        enc[1] = value & 0xFF;
        return 2;
    } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT16;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        return 3;
    } else if (value >= -((long long) 1 << 31) && value <= ((long long) 1 << 31) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT32;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        enc[3] = (value >> 16) & 0xFF;
        enc[4] = (value >> 24) & 0xFF;
        return 5;
    } else {
        return 0;
    }
}

int RdbEncoder::rdbTryIntegerEncoding(const std::string &string, unsigned char *enc) {
    int64_t value = str_to_int64(string);
    if (errno != 0) {
        return 0;
    }

    std::string newValue = str(value);

    if (newValue.length() != string.length() || newValue != string) return 0;

    return rdbEncodeInteger(value, enc);
}

int64_t RdbEncoder::rdbSaveLzfStringObject(const std::string &string) {
    size_t len = string.length();
    size_t comprlen, outlen;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) return 0;
    outlen = len - 4;

//    if ((out = zmalloc(outlen + 1)) == NULL) return 0;

    std::unique_ptr<void, cfree_delete<void>> out_m(malloc(outlen + 1));
    out = out_m.get();

    comprlen = lzf_compress(string.data(), len, out, outlen);
    if (comprlen == 0) {
        return 0;
    }

    int64_t nwritten = rdbSaveLzfBlob(out, comprlen, len);
    return nwritten;
}

int64_t RdbEncoder::rdbSaveLzfBlob(void *data, size_t compress_len, size_t original_len) {
    unsigned char byte;
    int64_t n, nwritten = 0;

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
