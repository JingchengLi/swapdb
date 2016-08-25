#ifndef NEMO_INCLUDE_NEMO_HASH_H
#define NEMO_INCLUDE_NEMO_HASH_H

#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

inline std::string EncodeHsizeKey(const rocksdb::Slice &name) {
    std::string buf;
    buf.append(1, DataType::kHSize);
    buf.append(name.data(), name.size());
    return buf;
}

inline int DecodeHsizeKey(const rocksdb::Slice &slice, std::string *name) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(name) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeHashKey(const rocksdb::Slice &name, const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kHash);
    buf.append(1, (uint8_t)name.size());
    buf.append(name.data(), name.size());
    buf.append(1, '=');
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeHashKey(const rocksdb::Slice &slice, std::string *name, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(name) == -1) {
        return -1;
    }
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(key) == -1) {
        return -1;
    }
    return 0;
}

}


#endif
