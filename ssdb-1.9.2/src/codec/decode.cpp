//
// Created by a1 on 16-11-3.
//
#include "decode.h"
#include "util/bytes.h"

int MetaKey::DecodeMetaKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != 'M'){
        return -1;
    }
    if (decoder.read_uint16(&slot) == -1){
        return -1;
    } else{
        slot = be16toh(slot);
    }
    return decoder.read_data(&key);
}

int ItemKey::DecodeItemKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != 'S'){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    return decoder.read_data(&field);
}

int ZScoreItemKey::DecodeItemKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != 'S'){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    uint64_t tscore = 0;
    if (decoder.read_uint64(&tscore) == -1){
        return -1;
    } else{
        tscore = be64toh(tscore);
        score = decodeScore(tscore);
    }
    return decoder.read_data(&field);
}

inline double decodeScore(const int64_t score) {
    return (double)(score - ZSET_SCORE_SHIFT) / 100000.0;
}

int ListItemKey::DecodeItemKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != 'S'){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    if (decoder.read_uint64(&seq) == -1){
        return -1;
    } else{
        seq = be64toh(seq);
    }
    return 0;
}

/*
 * decode meta value class
 */
int KvMetaVal::DecodeMetaVal(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    type = str[0];

    return decoder.read_data(&value);
}

int MetaVal::DecodeMetaVal(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    type = str[0];

    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }

    if(decoder.skip(1) == -1){
        return -1;
    }
    type = str[3];

    if (decoder.read_uint64(&length) == -1){
        return -1;
    } else{
        length = be64toh(length);
    }
    return 0;
}

int ListMetaVal::DecodeMetaVal(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    type = str[0];

    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }

    if(decoder.skip(1) == -1){
        return -1;
    }
    type = str[3];

    if (decoder.read_uint64(&length) == -1){
        return -1;
    } else{
        length = be64toh(length);
    }
    if (decoder.read_uint64(&left) == -1){
        return -1;
    } else{
        left = be64toh(left);
    }
    if (decoder.read_uint64(&right) == -1){
        return -1;
    } else{
        right = be64toh(right);
    }
    return 0;
}

/*
 * decode delete key class
 */
int DeleteKey::DecodeDeleteKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    } else{
        type = str[0];
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    if (decoder.read_uint16(&slot) == -1){
        return -1;
    } else{
        slot = be16toh(slot);
    }
    return decoder.read_data(&key);
}
