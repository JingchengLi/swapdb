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
#else
	cache_size = (size_t)conf.get_num("rocksdb.cache_size");
	block_size = (size_t)conf.get_num("rocksdb.block_size");
	max_open_files = (size_t)conf.get_num("rocksdb.max_open_files");
	write_buffer_size = (size_t)conf.get_num("rocksdb.write_buffer_size");
	compression = conf.get_str("rocksdb.compression");
#endif

	strtolower(&compression);
	if(compression != "no"){
		compression = "yes";
	}

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

}
