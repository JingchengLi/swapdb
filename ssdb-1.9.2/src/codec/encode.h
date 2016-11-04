//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_ENCODE_H
#define SSDB_ENCODE_H

#include "util.h"

string encode_meta_key(const string& key);

static string encode_key_internal(const string& key, const string& field, uint16_t version);

string encode_hash_key(const string& key, const string& field, uint16_t version);

string encode_set_key(const string& key, const string& member, uint16_t version);

string encode_zset_key(const string& key, const string& member, uint16_t version);

static uint64_t encodeScore(const double score);

string encode_zscore_key(const string& key, const string& member, double score, uint16_t version);

string encode_list_key(const string& key, uint64_t seq, uint16_t version);

/*
 * encode meta value
 */

string encode_kv_val(const string& val);

static string encode_meta_val_internal(const char type, uint64_t length, uint16_t version, char del);

string encode_hash_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_set_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_zset_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_list_meta_val(uint64_t length, uint64_t left, uint64_t right, uint16_t version = 0, char del = KEY_ENABLED_MASK);

/*
 * delete key
 */
string encode_delete_key(const string& key, uint16_t version = 0);

#endif //SSDB_ENCODE_H
