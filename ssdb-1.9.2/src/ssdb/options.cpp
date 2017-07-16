/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "options.h"
#include "../util/strings.h"

#ifdef USE_LEVELDB
Options::Options(){
    Config c;
    this->load(c);
}
#else
#endif

void Options::load(Config *conf) {
    if (conf == nullptr) {
        return;
    }
    c = conf;

#ifdef USE_LEVELDB
    cache_size = (size_t)conf->get_num("leveldb.cache_size");
    block_size = (size_t)conf->get_num("leveldb.block_size");
    compaction_speed = conf->get_num("leveldb.compaction_speed");
    max_open_files = (size_t)conf->get_num("leveldb.max_open_files");
    write_buffer_size = (size_t)conf->get_num("leveldb.write_buffer_size");
    compression = conf->get_str("leveldb.compression");


    if(cache_size <= 0){
        cache_size = 16;
    }
    if(block_size <= 0){
        block_size = 16;
    }
    if(write_buffer_size <= 0){
        write_buffer_size = 16;
    }
    if(max_open_files <= 0){
        max_open_files = 1000;
    }
    if(compaction_readahead_size <= 0){
        compaction_readahead_size = 4 * UNIT_MB;
    }

    if(max_bytes_for_level_base <= 0){
        max_bytes_for_level_base = 256 * UNIT_MB;
    }

    if(max_bytes_for_level_multiplier <= 0){
        max_bytes_for_level_multiplier = 10;
    }
#else
    expire_enable = conf->get_bool("server.expire_enable", false);

    cache_size = (size_t) conf->get_num("rocksdb.cache_size", 16);
    sim_cache = (size_t) conf->get_num("rocksdb.sim_cache", 0);
    block_size = (size_t) conf->get_num("rocksdb.block_size", 16);

    max_open_files = conf->get_num("rocksdb.max_open_files", 1000);
    write_buffer_size = conf->get_num("rocksdb.write_buffer_size", 16);

    compression = conf->get_bool("rocksdb.compression");
    rdb_compression = conf->get_bool("rocksdb.rdb_compression", false);
    transfer_compression = conf->get_bool("rocksdb.transfer_compression");
    level_compaction_dynamic_level_bytes = conf->get_bool("rocksdb.level_compaction_dynamic_level_bytes");
    use_direct_reads = conf->get_bool("rocksdb.use_direct_reads", false);
    optimize_filters_for_hits = conf->get_bool("rocksdb.optimize_filters_for_hits", false);
    cache_index_and_filter_blocks = conf->get_bool("rocksdb.cache_index_and_filter_blocks", false);

    compaction_readahead_size = (size_t) conf->get_num("rocksdb.compaction_readahead_size", 4);
    max_bytes_for_level_base = (size_t) conf->get_num("rocksdb.max_bytes_for_level_base", 256);
    max_bytes_for_level_multiplier = (size_t) conf->get_num("rocksdb.max_bytes_for_level_multiplier", 10);

    target_file_size_base = (size_t) conf->get_num("rocksdb.target_file_size_base", 64);

    min_write_buffer_number_to_merge = conf->get_num("rocksdb.min_write_buffer_number_to_merge", 2);
    max_write_buffer_number = conf->get_num("rocksdb.max_write_buffer_number", 3);
    max_background_flushes = conf->get_num("rocksdb.max_background_flushes", 4);
    max_background_compactions = conf->get_num("rocksdb.max_background_compactions", 4);

    level0_file_num_compaction_trigger = conf->get_num("rocksdb.level0_file_num_compaction_trigger", 4);
    level0_slowdown_writes_trigger = conf->get_num("rocksdb.level0_slowdown_writes_trigger", 20);
    level0_stop_writes_trigger = conf->get_num("rocksdb.level0_stop_writes_trigger", 36);


    upstream_ip = conf->get_str("upstream.ip");
    upstream_port = conf->get_num("upstream.port", 0);

#endif


}

std::ostream &operator<<(std::ostream &os, const Options &options) {
    os
            << "\n create_if_missing: " << options.create_if_missing
            << "\n create_missing_column_families: " << options.create_missing_column_families
            << "\n write_buffer_size: " << options.write_buffer_size

            << "\n max_open_files: " << options.max_open_files
            << "\n compression: " << options.compression

            << "\n rdb_compression: " << options.rdb_compression
            << "\n transfer_compression: " << options.transfer_compression

            << "\n level_compaction_dynamic_level_bytes: " << options.level_compaction_dynamic_level_bytes

            << "\n use_direct_reads: " << options.use_direct_reads
            << "\n optimize_filters_for_hits: " << options.optimize_filters_for_hits
            << "\n expire_enable: " << options.expire_enable

            << "\n max_write_buffer_number: " << options.max_write_buffer_number
            << "\n max_background_flushes: " << options.max_background_flushes
            << "\n max_background_compactions: " << options.max_background_compactions

            << "\n sim_cache: " << options.sim_cache
            << "\n cache_size: " << options.cache_size
            << "\n block_size: " << options.block_size
            << "\n compaction_readahead_size: " << options.compaction_readahead_size

            << "\n max_bytes_for_level_base: " << options.max_bytes_for_level_base
            << "\n max_bytes_for_level_multiplier: " << options.max_bytes_for_level_multiplier
            << "\n target_file_size_base: " << options.target_file_size_base

            << "\n level0_file_num_compaction_trigger: " << options.level0_file_num_compaction_trigger

            << "\n level0_slowdown_writes_trigger: " << options.level0_slowdown_writes_trigger

            << "\n level0_stop_writes_trigger: " << options.level0_stop_writes_trigger
            << "\n upstream_ip: " << options.upstream_ip
            << "\n upstream_port: " << options.upstream_port
            << "\n c: " << options.c;
    return os;
}
