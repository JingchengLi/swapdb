//
// Created by a1 on 16-11-2.
//
#include "encode.h"

string encode_meta_key(const string& key){
    string buf;
    unsigned char p[3] = {0};

    buf.append(1, 'M');
    uint16_t slot = (uint16_t)keyHashSlot(key.data(), (int)key.size());
    zipSaveInteger(p, slot, ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);

    buf.append(key);

    return buf;
}

string encode_key_internal(const string& key, const string& field, uint16_t version){
    string buf;
    unsigned char p[3] = {0};

    buf.append(1, 'S');
    zipSaveInteger(p, version, ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);

    zipSaveInteger(p, (int64_t)key.size(), ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);
    buf.append(key);

    buf.append(field);

    return buf;
}

string encode_hash_key(const string& key, const string& field, uint16_t version){
    return encode_key_internal(key, field, version);
}

string encode_set_key(const string& key, const string& member, uint16_t version){
    return encode_key_internal(key, member, version);
}

string encode_zset_key(const string& key, const string& member, uint16_t version){
    return encode_key_internal(key, member, version);
}

inline uint64_t encodeScore(const double score) {
    int64_t iscore;
    if (score < 0) {
        iscore = (int64_t)(score * 100000LL - 0.5) + ZSET_SCORE_SHIFT;
    } else {
        iscore = (int64_t)(score * 100000LL + 0.5) + ZSET_SCORE_SHIFT;
    }
    return (uint64_t)(iscore);
}

string encode_zscore_key(const string& key, const string& member, double score, uint16_t version){
    string buf;
    unsigned char p[3] = {0};

    buf.append(1, 'S');
    zipSaveInteger(p, version, ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);

    zipSaveInteger(p, (int64_t)key.size(), ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);
    buf.append(key);

    uint64_t new_score = encodeScore(score);
    new_score = htobe64(new_score);
    zipSaveInteger(p, new_score, ZIP_INT_64B); // 8 Byte

    buf.append(member);

    return buf;
}

string encode_list_key(const string& key, uint64_t seq, uint16_t version){
    string buf;
    unsigned char p[3] = {0};
    unsigned char seq_buf[9] = {0};

    buf.append(1, 'S');
    zipSaveInteger(p, version, ZIP_INT_16B); // 2 Bytes
    buf.append((const char*)p, 2);

    buf.append(key);

    zipSaveInteger(seq_buf, seq, ZIP_INT_64B);
    buf.append((const char*)seq_buf, 8);

    return buf;
}
