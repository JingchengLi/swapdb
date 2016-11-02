//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_ENCODE_H
#define SSDB_ENCODE_H

#include "util.h"
#include <string>
using namespace std;

#define ZSET_SCORE_SHIFT 1000000000000000000LL

string encode_meta_key(const string& key);

static string encode_key_internal(const string& key, const string& field, uint16_t version);

string encode_hash_key(const string& key, const string& field, uint16_t version);

string encode_set_key(const string& key, const string& member, uint16_t version);

string encode_zset_key(const string& key, const string& member, uint16_t version);

static uint64_t encodeScore(const double score);

string encode_zscore_key(const string& key, const string& member, double score, uint16_t version);

string encode_list_key(const string& key, uint64_t seq, uint16_t version);

#endif //SSDB_ENCODE_H
