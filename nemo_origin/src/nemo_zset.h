#ifndef NEMO_INCLUDE_NEMO_ZSET_H_
#define NEMO_INCLUDE_NEMO_ZSET_H_

#include <stdint.h>

#include "util.h"
#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

inline uint64_t EncodeScore(const double score) {
    int64_t iscore;
    if (score < 0) {
        iscore = (int64_t)(score * 100000LL - 0.5) + ZSET_SCORE_SHIFT;
    } else {
        iscore = (int64_t)(score * 100000LL + 0.5) + ZSET_SCORE_SHIFT;
    }
    return (uint64_t)(iscore);
}

inline double DecodeScore(const int64_t score) {
    return (double)(score - ZSET_SCORE_SHIFT) / 100000.0; 
}

inline std::string EncodeZSetKey(const rocksdb::Slice &key, const rocksdb::Slice &member) {
    std::string buf;
    buf.append(1, DataType::kZSet);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append(member.data(), member.size());
    return buf;
}

inline std::string EncodeZScorePrefix(const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kZScore);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    return buf;
}


inline int DecodeZSetKey(const rocksdb::Slice &slice, std::string *key, std::string *member) {
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

inline std::string EncodeZSizeKey(const rocksdb::Slice key) {
    std::string buf;
    buf.append(1, DataType::kZSize);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeZSizeKey(const rocksdb::Slice &slice, std::string *size) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(size) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeZScoreKey(const rocksdb::Slice &key, const rocksdb::Slice &member, const double score) {
    std::string buf;
    uint64_t new_score = EncodeScore(score);
    buf.append(1, DataType::kZScore);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    new_score = htobe64(new_score);
    buf.append((char *)&new_score, sizeof(int64_t));
    buf.append(member.data(), member.size());
    return buf;
}

inline int DecodeZScoreKey(const rocksdb::Slice &slice, std::string *key, std::string *member, double *score) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    uint64_t iscore = 0;
    decoder.ReadUInt64(&iscore);
    //iscore = be64toh(iscore);
    *score = DecodeScore(iscore);
    if (decoder.ReadData(member) == -1) {
        return -1;
    }
    return 0;
}

}
#endif

