/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_OPTION_H_
#define SSDB_OPTION_H_

#include <ostream>
#include "../util/config.h"

#ifdef USE_LEVELDB
class Options
{
public:
    Options();
    ~Options(){}

    void load(Config *conf);

    Options() {
        this->load(c);
    }

    Config* c = nullptr;

    size_t cache_size;
    size_t max_open_files;
    size_t write_buffer_size;
    size_t block_size;
    int compaction_speed;
    std::string compression;
    bool binlog;
    size_t binlog_capacity;

};
#else

#define UNIT_MB (1024 * 1024)
#define UNIT_KB (1024)

struct Options {
    bool create_if_missing = true;
    bool create_missing_column_families = true;
    int write_buffer_size = 64;
    int max_open_files = 5000;

    // default target_file_size_base and multiplier is the save as rocksdb
    bool compression = true;
    bool rdb_compression = true;
    bool transfer_compression = true;
    bool level_compaction_dynamic_level_bytes = false;
    bool use_direct_reads = false;
    bool optimize_filters_for_hits = false;
    bool cache_index_and_filter_blocks = false;
    bool expire_enable = false;

    int max_write_buffer_number = 3;
    int max_background_flushes = 4;
    int max_background_compactions = 4;

    size_t sim_cache = 100;
    size_t cache_size = 100;
    size_t block_size = 4;
    size_t compaction_readahead_size = 4;
    size_t max_bytes_for_level_base = 256;
    size_t max_bytes_for_level_multiplier = 10;

    size_t target_file_size_base = 64;

    int level0_file_num_compaction_trigger = 4; //start compaction
    int level0_slowdown_writes_trigger = 20; //slow write
    int level0_stop_writes_trigger = 36;  //block write

    std::string upstream_ip;
    int upstream_port = 0;

    void load(Config *conf);

    Options() {
        this->load(c);
    }

    Config* c = nullptr;

    friend std::ostream &operator<<(std::ostream &os, const Options &options);
};

#endif


#endif
