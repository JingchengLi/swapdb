//
// Created by a1 on 16-11-2.
//
#include "encode.h"
#include "ssdb/const.h"

string encode_meta_key(const string& key){
    string buf;

    buf.append(1, 'M');
    uint16_t slot = (uint16_t)keyHashSlot(key.data(), (int)key.size());
    slot = htobe16(slot);
    buf.append((char *)&slot, sizeof(uint16_t));

    buf.append(key);

    return buf;
}

string encode_key_internal(const string& key, const string& field, uint16_t version){
    string buf;

    buf.append(1, 'S');
    version = htobe16(version);
    buf.append((char *)&version, sizeof(uint16_t));

    uint16_t len = htobe16((uint16_t)key.size());
    buf.append((char *)&len, sizeof(uint16_t));
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

    buf.append(1, 'S');
    version = htobe16(version);
    buf.append((char *)&version, sizeof(uint16_t));

    uint16_t len = htobe16((uint16_t)key.size());
    buf.append((char *)&len, sizeof(uint16_t));
    buf.append(key);

    uint64_t new_score = encodeScore(score);
    new_score = htobe64(new_score);
    buf.append((char *)&new_score, sizeof(uint64_t));

    buf.append(member);

    return buf;
}

string encode_list_key(const string& key, uint64_t seq, uint16_t version){
    string buf;

    buf.append(1, 'S');
    version = htobe16(version);
    buf.append((char *)&version, sizeof(uint16_t));

    buf.append(key);

    seq = htobe64(seq);
    buf.append((char *)&seq, sizeof(uint64_t));

    return buf;
}

string encode_kv_val(const string& val){
    string buf;
    buf.append(1, DataType::KV);
    buf.append(val);
    return buf;
}

string encode_meta_val_internal(const char type, uint64_t length, uint16_t version, char del){
    string buf;

    buf.append(1, type);
    version = htobe16(version);
    buf.append((char *)&version, sizeof(uint16_t));

    buf.append(1, del);

    length = htobe64(length);
    buf.append((char *)&length, sizeof(uint64_t));

    return buf;
}

string encode_hash_meta_val(uint64_t length, uint16_t version, char del){
    return encode_meta_val_internal(DataType::HSIZE, length, version, del);
}

string encode_set_meta_val(uint64_t length, uint16_t version, char del){
    return encode_meta_val_internal(DataType::SSIZE, length, version, del);
}

string encode_zset_meta_val(uint64_t length, uint16_t version, char del){
    return encode_meta_val_internal(DataType::ZSIZE, length, version, del);
}

string encode_list_meta_val(uint64_t length, uint64_t left, uint64_t right, uint16_t version, char del){
    string buf;

    buf.append(1, DataType::LSZIE);
    version = htobe16(version);
    buf.append((char *)&version, sizeof(uint16_t));

    buf.append(1, del);

    length = htobe64(length);
    buf.append((char *)&length, sizeof(uint64_t));

    left = htobe64(left);
    buf.append((char *)&left, sizeof(uint64_t));

    right = htobe64(right);
    buf.append((char *)&right, sizeof(uint64_t));

    return buf;
}