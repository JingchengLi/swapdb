#ifndef NEMO_INCLUDE_NEMO_SET_H_
#define NEMO_INCLUDE_NEMO_SET_H_

#include <stdint.h>

#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

inline std::string EncodeSetKey(const rocksdb::Slice &key, const rocksdb::Slice &member) {
    std::string buf;
    buf.append(1, DataType::kSet);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append(member.data(), member.size());
    return buf;
}

inline int DecodeSetKey(const rocksdb::Slice &slice, std::string *key, std::string *member) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.ReadData(member) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeSSizeKey(const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kSSize);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeSSizeKey(const rocksdb::Slice &slice, std::string *size) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(size) == -1) {
        return -1;
    }
    return 0;
}

}
#endif

