#ifndef NEMO_INCLUDE_NEMO_LIST_H
#define NEMO_INCLUDE_NEMO_LIST_H

#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

struct ListData {
  int64_t priv;
  int64_t next;
  std::string val;
  ListData() : priv(0), next(0) {}
  ListData(int64_t _priv, int64_t _next, const std::string& _value)
    : priv(_priv), next(_next), val(_value) {}
  void reset(int64_t _priv, int64_t _next, const std::string& _value) {
    priv = _priv;
    next = _next;
    val = _value;
  }
};

inline std::string EncodeLMetaKey(const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kLMeta);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeLMetaKey(const rocksdb::Slice &slice, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(key) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeListKey(const rocksdb::Slice &key, const int64_t seq) {
    std::string buf;
    buf.append(1, DataType::kList);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append((char *)&seq, sizeof(int64_t));
    return buf;
}

inline int DecodeListKey(const rocksdb::Slice &slice, std::string *key, int64_t *seq) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.ReadInt64(seq) == -1) {
        return -1;
    }
    return 0;
}

inline void EncodeListVal(const std::string &raw_val, const int64_t priv, const int64_t next, std::string &en_val) {
    en_val.clear();
    en_val.append((char *)&priv, sizeof(int64_t));
    en_val.append((char *)&next, sizeof(int64_t));
    en_val.append(raw_val.data(), raw_val.size());
}

inline void DecodeListVal(const std::string &en_val, int64_t *priv, int64_t *next, std::string &raw_val) {
    raw_val.clear();
    *priv = *((int64_t *)en_val.data());
    *next = *((int64_t *)(en_val.data() + sizeof(int64_t)));
    raw_val = en_val.substr(sizeof(int64_t) * 2, en_val.size() - sizeof(int64_t) * 2);
}

}

#endif
