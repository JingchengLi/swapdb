//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_ENCODE_H
#define SSDB_ENCODE_H

#include "util.h"

class Bytes;

string encode_meta_key(const Bytes& key);

string encode_hash_key(const Bytes& key, const Bytes& field, uint16_t version);

string encode_set_key(const Bytes& key, const Bytes& member, uint16_t version);

void update_list_key(std::string& old, uint64_t seq);

string encode_list_key(const Bytes& key, uint64_t seq, uint16_t version);

string encode_zset_key(const Bytes& key, const Bytes& member, uint16_t version);

string encode_zscore_prefix(const Bytes &key, uint16_t version);

string encode_zscore_key(const Bytes& key, const Bytes& field, double score, uint16_t version);

string encode_eset_key(const Bytes& member);

string encode_escore_key(const Bytes& member, uint64_t score);

/*
 * encode meta value
 */

string encode_kv_val( const Bytes& val, uint16_t version, char del = KEY_ENABLED_MASK);

string encode_hash_meta_val(uint64_t length, uint16_t version, char del = KEY_ENABLED_MASK);

string encode_set_meta_val(uint64_t length, uint16_t version, char del = KEY_ENABLED_MASK);

string encode_zset_meta_val(uint64_t length, uint16_t version, char del = KEY_ENABLED_MASK);

string encode_list_meta_val(uint64_t length, uint64_t left, uint64_t right, uint16_t version, char del = KEY_ENABLED_MASK);

/*
 * delete key
 */
string encode_delete_key(const Bytes& key, uint16_t version);


/*
 * repo key
 */
string encode_repo_key();

string encode_repo_item(uint64_t timestamp, uint64_t index);


uint64_t encodeScore(const double score);

#endif //SSDB_ENCODE_H
