//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_ENCODE_H
#define SSDB_ENCODE_H

#include "util.h"
#include "util/bytes.h"

string encode_meta_key(const Bytes& key);

string encode_hash_key(const Bytes& key, const Bytes& field, uint16_t version = 0);

string encode_set_key(const Bytes& key, const Bytes& member, uint16_t version = 0);

string encode_zset_key(const Bytes& key, const Bytes& member, uint16_t version = 0);

string encode_zset_score_prefix(const Bytes& key, uint16_t version);

string encode_eset_key(const Bytes& member);

string encode_zscore_key(const Bytes& key, const Bytes& field, double score, uint16_t version = 0);

string encode_escore_key(const Bytes& member, uint64_t score);

string encode_list_key(const Bytes& key, uint64_t seq, uint16_t version = 0);

/*
 * encode meta value
 */

string encode_kv_val( const Bytes& val, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_hash_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_set_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_zset_meta_val(uint64_t length, uint16_t version = 0, char del = KEY_ENABLED_MASK);

string encode_list_meta_val(uint64_t length, uint64_t left, uint64_t right, uint16_t version = 0, char del = KEY_ENABLED_MASK);

/*
 * delete key
 */
string encode_delete_key(const Bytes& key, uint16_t version = 0);

string encode_bqueue_key(uint16_t task_type, const Bytes& val);

#endif //SSDB_ENCODE_H
