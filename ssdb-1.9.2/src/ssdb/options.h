/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_OPTION_H_
#define SSDB_OPTION_H_

#include "../util/config.h"

#ifdef USE_LEVELDB
class Options
{
public:
	Options();
	~Options(){}
	
	void load(const Config &conf);

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
struct Options {
    bool create_if_missing;
    int write_buffer_size;
    int max_open_files;
    bool use_bloomfilter;
    int write_threads;

    // default target_file_size_base and multiplier is the save as rocksdb
    int target_file_size_base;
    int target_file_size_multiplier;
    std::string compression;
    int max_background_flushes;
    int max_background_compactions;

	size_t cache_size;
	size_t block_size;


	//=========begin
    // for ssdb
    void load(const Config &conf);
    bool binlog;
    size_t binlog_capacity;
    //=========end

    Options() : create_if_missing(true),
                write_buffer_size(4 * 1024 * 1024),
                max_open_files(5000),
                use_bloomfilter(true),
                write_threads(71),
                target_file_size_base(2 * 1024 * 1024),
                target_file_size_multiplier(1),
                compression("yes"),
                max_background_flushes(1),
                max_background_compactions(1) {
        Config c;
        this->load(c);
    }
};
#endif



class RedisUpstream
{
public:
	RedisUpstream(const std::string &ip, int port) : ip(ip), port(port) {}

	~RedisUpstream(){}

	std::string ip;
	int port;
};

#endif
