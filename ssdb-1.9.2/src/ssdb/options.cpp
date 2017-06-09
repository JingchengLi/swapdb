/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "options.h"
#include "../util/strings.h"

#ifdef NDEBUG
	static const int LOG_QUEUE_SIZE  = 20 * 1000 * 1000;
#else
	static const int LOG_QUEUE_SIZE  = 10000;
#endif

#ifdef USE_LEVELDB
Options::Options(){
	Config c;
	this->load(c);
}
#else
#endif

void Options::load(const Config &conf){
#ifdef USE_LEVELDB
	cache_size = (size_t)conf.get_num("leveldb.cache_size");
	block_size = (size_t)conf.get_num("leveldb.block_size");
	compaction_speed = conf.get_num("leveldb.compaction_speed");
	max_open_files = (size_t)conf.get_num("leveldb.max_open_files");
	write_buffer_size = (size_t)conf.get_num("leveldb.write_buffer_size");
	compression = conf.get_str("leveldb.compression");


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
	cache_size = (size_t)conf.get_num("rocksdb.cache_size", 16);
	block_size = (size_t)conf.get_num("rocksdb.block_size", 16);

	max_open_files = conf.get_num("rocksdb.max_open_files", 1000);
	write_buffer_size = conf.get_num("rocksdb.write_buffer_size", 16);
	compression = conf.get_str("rocksdb.compression");
	level_compaction_dynamic_level_bytes = conf.get_str("rocksdb.level_compaction_dynamic_level_bytes");
	use_direct_reads = conf.get_str("rocksdb.use_direct_reads");

	compaction_readahead_size = (size_t)conf.get_num("rocksdb.compaction_readahead_size", 4);
	max_bytes_for_level_base = (size_t)conf.get_num("rocksdb.max_bytes_for_level_base", 256);
	max_bytes_for_level_multiplier = (size_t)conf.get_num("rocksdb.max_bytes_for_level_multiplier", 10);

	target_file_size_base = (size_t)conf.get_num("rocksdb.target_file_size_base", 64);

	max_background_cd_threads = conf.get_num("rocksdb.max_background_cd_threads", 4);

	level0_file_num_compaction_trigger = conf.get_num("rocksdb.level0_file_num_compaction_trigger", 4);
	level0_slowdown_writes_trigger = conf.get_num("rocksdb.level0_slowdown_writes_trigger", 20);
	level0_stop_writes_trigger = conf.get_num("rocksdb.level0_stop_writes_trigger", 36);

#endif



	strtolower(&compression);
	if(compression != "no"){
		compression = "yes";
	}

	strtolower(&level_compaction_dynamic_level_bytes);
	if(level_compaction_dynamic_level_bytes != "yes"){
		level_compaction_dynamic_level_bytes = "no";
	}

	strtolower(&use_direct_reads);
	if(use_direct_reads != "yes"){
		use_direct_reads = "no";
	}




}
