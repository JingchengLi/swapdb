//
// Created by a1 on 16-11-3.
//
#include "decode.h"
#include "util/bytes.h"

static double decodeScore(const int64_t score);

int MetaKey::DecodeMetaKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != DataType::META){
        return -1;
    }
    if (decoder.read_uint16(&slot) == -1){
        return -1;
    } else{
        slot = be16toh(slot);
    }
    decoder.read_data(&key);
    return 0;
}

int ItemKey::DecodeItemKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != 'S'){
        return -1;
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    decoder.read_data(&field);
    return 0;
}

int ZScoreItemKey::DecodeItemKey(const string &str) {
    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != DataType::ZSCORE){
        return -1;
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    uint64_t tscore = 0;
    if (decoder.read_uint64(&tscore) == -1){
        return -1;
    } else{
        tscore = be64toh(tscore);
        score = decodeScore(tscore);
    }
    decoder.read_data(&field);
    return 0;
}

static double decodeScore(const int64_t score) {
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
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    if (decoder.read_uint64(&seq) == -1){
        return -1;
    } else{
        seq = be64toh(seq);
    }
    return 0;
}

int EScoreItemKey::DecodeItemKey(const string &str) {

    Decoder decoder(str.data(), str.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if (str[0] != DataType::ESCORE){
        return -1;
    }

    uint64_t tscore = 0;
    if (decoder.read_uint64(&tscore) == -1){
        return -1;
    } else{
        tscore = be64toh(tscore);
        score = (int64_t) tscore;
    }

    decoder.read_data(&field);

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

    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }

    if(decoder.skip(1) == -1){
        return -1;
    }
    del = str[3];
    if((del != KEY_DELETE_MASK) && (del != KEY_ENABLED_MASK)){
        return -1;
    } else if (del == KEY_DELETE_MASK){
        return 0;
    }
    if (type != DataType::KV){
        return -1;
    }

    decoder.read_data(&value);
    return 0;
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
    del = str[3];
    if((del != KEY_DELETE_MASK) && (del != KEY_ENABLED_MASK)){
        return -1;
    } else if (del == KEY_DELETE_MASK){
        return 0;
    }
    if ((type != DataType::HSIZE) && (type != DataType::SSIZE) && (type != DataType::ZSIZE)){
        return -1;
    }

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
    del = str[3];
    if((del != KEY_DELETE_MASK) && (del != KEY_ENABLED_MASK)){
        return -1;
    } else if (del == KEY_DELETE_MASK){
        return 0;
    }
    if (type != DataType::LSIZE){
        return -1;
    }

    if (decoder.read_uint64(&length) == -1){
        return -1;
    } else{
        length = be64toh(length);
    }
    if (decoder.read_uint64(&left_seq) == -1){
        return -1;
    } else{
        left_seq = be64toh(left_seq);
    }
    if (decoder.read_uint64(&right_seq) == -1){
        return -1;
    } else{
        right_seq = be64toh(right_seq);
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
        if ((type = str[0]) != KEY_DELETE_MASK){
            return -1;
        }
    }
    if (decoder.read_uint16(&slot) == -1){
        return -1;
    } else{
        slot = be16toh(slot);
    }
    if (decoder.read_16_data(&key) == -1){
        return -1;
    }
    if (decoder.read_uint16(&version) == -1){
        return -1;
    } else{
        version = be16toh(version);
    }
    std::string tmp;
    decoder.read_data(&tmp);
    if (tmp.size() == 1){
        key_type = tmp[0];
    } else{
        return -1;
    }
    return 0;
}
