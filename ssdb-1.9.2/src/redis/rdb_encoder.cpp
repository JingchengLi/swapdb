//
// Created by zts on 16-12-20.
//

#include <cmath>
#include <cstring>
#include "rdb_encoder.h"

void RdbEncoder::encodeFooter() {

    unsigned char buf[2];
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    w.append((char *) &buf, 2);


    uint64_t crc;
    crc = crc64(0, (unsigned char *) w.data(), w.length());
    memrev64ifbe(&crc);
    w.append((char *) &crc, 8);
}

int RdbEncoder::saveLen(uint32_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        if (writeRaw(buf, 1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        if (writeRaw(buf, 2) == -1) return -1;
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        if (writeRaw(buf, 1) == -1) return -1;
        uint32_t len32 = htonl(len);
        if (writeRaw(&len32, 4) == -1) return -1;
        nwritten = 1 + 4;
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        if (writeRaw(buf, 1) == -1) return -1;
        len = htonu64(len);
        if (writeRaw(&len, 8) == -1) return -1;
        nwritten = 1 + 8;
    }

    return 1;
}

void RdbEncoder::saveType(unsigned char type) {
    w.append(1, type);
}

std::string RdbEncoder::toString() const {
    return w;
}

int RdbEncoder::encodeString(const std::string &string) {
    int ret = encodeIntString(string);
    if (ret == -1) {
        return ret;
    }

    if (ret == 1) {
        return ret;
    }

    saveLen(string.length());
    writeRaw((void *) string.data(), string.length());

    ret = 1;
    return ret;
}


int RdbEncoder::encodeIntString(const std::string &string) {

    //TODO

    return 0;
}

int RdbEncoder::saveRawString(const std::string &string) {

    saveLen(string.length());
    writeRaw((void *) string.data(), string.length());

    return 1;
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
        snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = std::strlen((char*)buf+1);
        len = buf[0]+1;
    }

    w.append((char *) &buf, len);

    return 1;
}
