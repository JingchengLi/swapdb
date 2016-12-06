/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#ifdef USE_LEVELDB
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#else
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#define leveldb rocksdb
#endif

#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"

SSDBImpl::SSDBImpl()
	: bg_cv_(&mutex_bgtask_){
	ldb = NULL;
	binlogs = NULL;
	this->bgtask_flag_ = true;
}

SSDBImpl::~SSDBImpl(){
	if(binlogs){
		delete binlogs;
	}
	if(ldb){
		delete ldb;
	}
//	if(expiration){
//		delete expiration;
//	}
	Locking l(&this->mutex_bgtask_);
	this->stop();
#ifdef USE_LEVELDB
	if(options.block_cache){
		delete options.block_cache;
	}
	if(options.filter_policy){
		delete options.filter_policy;
	}
#endif
}

SSDB* SSDB::open(const Options &opt, const std::string &dir){
	SSDBImpl *ssdb = new SSDBImpl();
	ssdb->options.create_if_missing = true;
	ssdb->options.max_open_files = opt.max_open_files;
#ifdef USE_LEVELDB
	ssdb->options.filter_policy = leveldb::NewBloomFilterPolicy(10);
	ssdb->options.block_cache = leveldb::NewLRUCache(opt.cache_size * 1048576);
	ssdb->options.block_size = opt.block_size * 1024;
	ssdb->options.compaction_speed = opt.compaction_speed;
#endif
	ssdb->options.write_buffer_size = opt.write_buffer_size * 1024 * 1024;
	if(opt.compression == "yes"){
		ssdb->options.compression = leveldb::kSnappyCompression;
	}else{
		ssdb->options.compression = leveldb::kNoCompression;
	}

	leveldb::Status status;

	status = leveldb::DB::Open(ssdb->options, dir, &ssdb->ldb);
	if(!status.ok()){
		log_error("open db failed: %s", status.ToString().c_str());
		goto err;
	}
	ssdb->binlogs = new BinlogQueue(ssdb->ldb, opt.binlog, opt.binlog_capacity);
//    ssdb->expiration = new ExpirationHandler(ssdb); //todo 后续如果支持set命令中设置过期时间，添加此行，同时删除serv.cpp中相应代码
    ssdb->start();

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

int SSDBImpl::flushdb(){
	Transaction trans(binlogs);
	int ret = 0;
	bool stop = false;
	while(!stop){
		leveldb::Iterator *it;
		leveldb::ReadOptions iterate_options;
		iterate_options.fill_cache = false;
		leveldb::WriteOptions write_opts;

		it = ldb->NewIterator(iterate_options);
		it->SeekToFirst();
		for(int i=0; i<10000; i++){
			if(!it->Valid()){
				stop = true;
				break;
			}
			//log_debug("%s", hexmem(it->key().data(), it->key().size()).c_str());
			leveldb::Status s = ldb->Delete(write_opts, it->key());
			if(!s.ok()){
				log_error("del error: %s", s.ToString().c_str());
				stop = true;
				ret = -1;
				break;
			}
			it->Next();
		}
		delete it;
	}
	binlogs->flush();
	return ret;
}

Iterator* SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
//	if(it->Valid() && it->key() == start){
//		it->Next();
//	}
	return new Iterator(it, end, limit);
}

Iterator* SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(!it->Valid()){
		it->SeekToLast();
	}else{
		it->Prev();
	}
	return new Iterator(it, end, limit, Iterator::BACKWARD);
}

/* raw operates */

int SSDBImpl::raw_set(const Bytes &key, const Bytes &val){
	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Put(write_opts, slice(key), slice(val));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_del(const Bytes &key){
	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Delete(write_opts, slice(key));
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_get(const Bytes &key, std::string *val){
	leveldb::ReadOptions opts;
	opts.fill_cache = false;
	leveldb::Status s = ldb->Get(opts, slice(key), val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

uint64_t SSDBImpl::size(){
	// todo r2m adaptation
	std::string s = "A";
	std::string e(1, 'z' + 1);
	leveldb::Range ranges[1];
	ranges[0] = leveldb::Range(s, e);
	uint64_t sizes[1];
	ldb->GetApproximateSizes(ranges, 1, sizes);
	return sizes[0];
}

std::vector<std::string> SSDBImpl::info(){
	//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "leveldb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "leveldb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	std::vector<std::string> info;
	std::vector<std::string> keys;
	/*
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "leveldb.num-files-at-level%d", i);
		keys.push_back(buf);
	}
	*/
	keys.push_back("leveldb.stats");
	//keys.push_back("leveldb.sstables");

	for(size_t i=0; i<keys.size(); i++){
		std::string key = keys[i];
		std::string val;
		if(ldb->GetProperty(key, &val)){
			info.push_back(key);
			info.push_back(val);
		}
	}

	return info;
}

void SSDBImpl::compact(){
	ldb->CompactRange(NULL, NULL);
}

// todo r2m adaptation
int SSDBImpl::key_range(std::vector<std::string> *keys){
	int ret = 0;
	std::string kstart, kend;
	std::string hstart, hend;
	std::string zstart, zend;
	std::string qstart, qend;
	
	Iterator *it;
	
	it = this->iterator(encode_kv_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_kv_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_hsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_hsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_zsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_zsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_zsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_zsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_qsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			if(decode_qsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				qstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_qsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			if(decode_qsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				qend = n;
			}
		}
	}
	delete it;

	keys->push_back(kstart);
	keys->push_back(kend);
	keys->push_back(hstart);
	keys->push_back(hend);
	keys->push_back(zstart);
	keys->push_back(zend);
	keys->push_back(qstart);
	keys->push_back(qend);
	
	return ret;
}

void SSDBImpl::start() {
	int err = pthread_create(&bg_tid_, NULL, &thread_func, this);
	if(err != 0){
		log_fatal("can't create thread: %s", strerror(err));
		exit(0);
	}
}

void SSDBImpl::stop() {
	this->bgtask_flag_ = false;
}

void SSDBImpl::load_delete_keys_from_db(int num) {
    std::string start;
    start.append(1, 'D');
    auto it = std::unique_ptr<Iterator>(this->iterator(start, "", num));
    while (it->next()){
        if (it->key().String()[0] != KEY_DELETE_MASK){
            break;
        }
        tasks_.push(it->key().String());
    }
}

int SSDBImpl::delete_meta_key(const DeleteKey& dk, leveldb::WriteBatch& batch) {
    std::string meta_key = encode_meta_key(dk.key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (!s.ok() && !s.IsNotFound()){
        return -1;
    } else if(s.ok()){
        Decoder decoder(meta_val.data(), meta_val.size());
        if(decoder.skip(1) == -1){
            return -1;
        }

        uint16_t version = 0;
        if (decoder.read_uint16(&version) == -1){
            return -1;
        } else{
            version = be16toh(version);
        }

        char del = meta_val[3];

        if (del == KEY_DELETE_MASK && version == dk.version){
            batch.Delete(meta_key);
        }
    }
    return 0;
}

void SSDBImpl::delete_key_loop(const std::string &del_key) {
    Transaction trans(binlogs);

    DeleteKey dk;
    if(dk.DecodeDeleteKey(del_key) == -1){
        log_fatal("delete key error!");
        return;
    }

//    char log_type=BinlogType::SYNC;
    std::string start = encode_hash_key(dk.key, "", dk.version);
    auto it = std::unique_ptr<Iterator>(this->iterator(start, "", -1));
	leveldb::WriteBatch batch;
    while (it->next()){
        ItemKey ik;
        std::string item_key = it->key().String();
        if (ik.DecodeItemKey(item_key) == -1){
            log_fatal("decode delete key error!");
            break;
        }
        if (ik.key == dk.key && ik.version == dk.version){
			batch.Delete(item_key);
        } else{
            break;
        }
    }
	batch.Delete(del_key);
    if (delete_meta_key(dk, batch) == -1){
        log_fatal("delete meta key error!");
        return;
    }

	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Write(write_opts, &batch);
    if (!s.ok()){
        log_fatal("SSDBImpl::delKey Backend Task error!");
        return ;
    }
}

void SSDBImpl::runBGTask() {
	while (bgtask_flag_){
		mutex_bgtask_.lock();
        if (tasks_.empty()){
            load_delete_keys_from_db(1000);
            if (tasks_.empty()){
                mutex_bgtask_.unlock();
                usleep(1000 * 1000);
				continue;
            }
        }

        std::string del_key;
        if (!tasks_.empty()){
            del_key = tasks_.front();
            tasks_.pop();
        }
        mutex_bgtask_.unlock();

        if (!del_key.empty()){
            delete_key_loop(del_key);
        }
	}
}

void* SSDBImpl::thread_func(void *arg) {
    SSDBImpl *bg_task = (SSDBImpl *)arg;

	bg_task->runBGTask();

	return (void *)NULL;
}
