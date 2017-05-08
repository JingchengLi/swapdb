/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "../util/PTimer.h"
#ifdef USE_LEVELDB
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#else
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "rocksdb/convenience.h"

#include "t_listener.h"
#define leveldb rocksdb
#endif

#include "iterator.h"
#include "ssdb_impl.h"

SSDBImpl::SSDBImpl()
	: bg_cv_(&mutex_bgtask_){
	ldb = NULL;
	this->bgtask_flag_ = true;
	expiration = NULL;
}

SSDBImpl::~SSDBImpl(){
    if(expiration){
        delete expiration;
    }

    this->stop();

    if(ldb){
        delete ldb;
    }
    log_info("SSDBImpl finalized");

#ifdef USE_LEVELDB
    //auto memory man in rocks
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
#else
	rocksdb::BlockBasedTableOptions op;

	//for spin disk
//	op.cache_index_and_filter_blocks = true;
//  	ssdb->options.optimize_filters_for_hits = true;
//	ssdb->options.skip_stats_update_on_db_open = true;
//	ssdb->options.level_compaction_dynamic_level_bytes = true;
//	ssdb->options.new_table_reader_for_compaction_inputs = true;
//	ssdb->options.compaction_readahead_size = 16 * 1024 * 1024;
//
//	ssdb->options.level0_file_num_compaction_trigger = 3; //start compaction
//	ssdb->options.level0_slowdown_writes_trigger = 20; //slow write
//	ssdb->options.level0_stop_writes_trigger = 24;  //block write
	//========
//	ssdb->options.target_file_size_base = 2 * 1024 * 1024; //sst file target size

    op.filter_policy = std::shared_ptr<const leveldb::FilterPolicy>(leveldb::NewBloomFilterPolicy(10));
    op.block_cache = leveldb::NewLRUCache(opt.cache_size * 1048576);
	op.block_size = opt.block_size * 1024;

	ssdb->options.table_factory = std::shared_ptr<leveldb::TableFactory>(rocksdb::NewBlockBasedTableFactory(op));
#endif
	ssdb->options.write_buffer_size = static_cast<size_t >(opt.write_buffer_size) * 1024 * 1024;
	if(opt.compression == "yes"){
		ssdb->options.compression = leveldb::kSnappyCompression;
	}else{
		ssdb->options.compression = leveldb::kNoCompression;
	}

#ifdef USE_LEVELDB
#else
	ssdb->options.listeners.push_back(std::shared_ptr<t_listener>(new t_listener()));
#endif

	leveldb::Status status;

	status = leveldb::DB::Open(ssdb->options, dir, &ssdb->ldb);
	if(!status.ok()){
		log_error("open db failed: %s", status.ToString().c_str());
		goto err;
	}
    ssdb->expiration = new ExpirationHandler(ssdb); //todo 后续如果支持set命令中设置过期时间，添加此行，同时删除serv.cpp中相应代码
    ssdb->start();

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

int SSDBImpl::flushdb(){
//lock

    Locking<RecordMutex<Mutex>> gl(&mutex_record_);
	redisCursorService.ClearAllCursor();

#ifdef USE_LEVELDB

#else
    leveldb::Slice begin("0");
    leveldb::Slice end("z");
    leveldb::DeleteFilesInRange(ldb, ldb->DefaultColumnFamily(), &begin, &end);
#endif


    uint64_t total = 0;
	int ret = 1;
	bool stop = false;

	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	leveldb::WriteOptions write_opts;

	unique_ptr<leveldb::Iterator> it = unique_ptr<leveldb::Iterator>(ldb->NewIterator(iterate_options));

	it->SeekToFirst();

	while(!stop){

		leveldb::WriteBatch writeBatch;

		for(int i=0; i<100000; i++){
			if(!it->Valid()){
				stop = true;
				break;
			}

            total ++;
            writeBatch.Delete(it->key());
			it->Next();
		}

 		leveldb::Status s = ldb->Write(write_opts, &writeBatch);
		if(!s.ok()){
			log_error("del error: %s", s.ToString().c_str());
			stop = true;
			ret = -1;
		}

	}


//#ifdef USE_LEVELDB
//
//#else
//	leveldb::CompactRangeOptions compactRangeOptions = leveldb::CompactRangeOptions();
//	ldb->CompactRange(compactRangeOptions, &begin, &end);
//#endif

	log_info("[flushdb] %d keys deleted by iteration", total);

	return ret;
}

Iterator* SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit,
							 const leveldb::Snapshot *snapshot){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	if (snapshot) {
		iterate_options.snapshot = snapshot;
	}
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
//	if(it->Valid() && it->key() == start){
//		it->Next();
//	}
	return new Iterator(it, end, limit, Iterator::FORWARD, iterate_options.snapshot);
}

Iterator* SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit,
								 const leveldb::Snapshot *snapshot){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	if (snapshot) {
		iterate_options.snapshot = snapshot;
	}
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(!it->Valid()){
		it->SeekToLast();
	}else{
        if (memcmp(start.data(), it->key().data(), start.size()) != 0){
            it->Prev();
        }
	}
	return new Iterator(it, end, limit, Iterator::BACKWARD, iterate_options.snapshot);
}

const leveldb::Snapshot* SSDBImpl::GetSnapshot() {
    if (ldb) {
        return ldb->GetSnapshot();
    }
    return nullptr;
}

void SSDBImpl::ReleaseSnapshot(const leveldb::Snapshot* snapshot) {
	if (snapshot != nullptr) {
		ldb->ReleaseSnapshot(snapshot);
	}
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
#ifdef USE_LEVELDB
//    std::string s = "A";
    std::string s(1, DataType::META);
    std::string e(1, DataType::META + 1);
	leveldb::Range ranges[1];
	ranges[0] = leveldb::Range(s, e);
	uint64_t sizes[1];
	ldb->GetApproximateSizes(ranges, 1, sizes);
	return (sizes[0] / 18);
#else
    std::string num = "0";
    ldb->GetProperty("rocksdb.estimate-num-keys", &num);

    return Bytes(num).Uint64();
#endif

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
#ifdef USE_LEVELDB
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "leveldb.num-files-at-level%d", i);
		keys.push_back(buf);
	}

	keys.push_back("leveldb.stats");
	keys.push_back("leveldb.sstables");

#else
    for(int i=0; i<7; i++){
        char buf[128];
        snprintf(buf, sizeof(buf), "rocksdb.num-files-at-level%d", i);
        keys.push_back(buf);
    }

    keys.push_back("rocksdb.stats");
    keys.push_back("rocksdb.sstables");

#endif

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
#ifdef USE_LEVELDB
	ldb->CompactRange(NULL, NULL);
#else
	leveldb::CompactRangeOptions compactRangeOptions = rocksdb::CompactRangeOptions();
	ldb->CompactRange(compactRangeOptions, NULL, NULL);
#endif
}


leveldb::Status SSDBImpl::CommitBatch(const leveldb::WriteOptions& options, leveldb::WriteBatch *updates) {

	leveldb::Status s = ldb->Write(options, updates);

	return s;
}

leveldb::Status SSDBImpl::CommitBatch(leveldb::WriteBatch *updates) {

	if (recievedIndex > 0) {
		//role is slave
		if (commitedIndex > 0) {
			//role is slave
		} else {
			log_info("role change master -> slave");
		}

		updates->Put(encode_repo_key(), encode_repo_item(recievedTimestamp, recievedIndex));

	} else if (recievedIndex == -1) {

		log_error("wtf??????????????? reset recievedIndex");
		resetRecievedInfo();

		updates->Put(encode_repo_key(), encode_repo_item(recievedTimestamp, recievedIndex));

	} else {
		//recievedIndex == 0

		if (commitedIndex > 0) {
			log_info("ole change slave -> master");

			resetRecievedInfo();

			updates->Put(encode_repo_key(), encode_repo_item(recievedTimestamp, recievedIndex));

		} else {
			//role is master
		}

	}

	leveldb::Status s = ldb->Write(leveldb::WriteOptions(), updates);

	if (s.ok()) {
		//update
		updateCommitedInfo(recievedTimestamp, recievedIndex);
		resetRecievedInfo();

	} else {
		updateRecievedInfo(-1, -1);
	}

	return s;
}


void SSDBImpl::start() {
    this->bgtask_flag_ = true;
	int err = pthread_create(&bg_tid_, NULL, &thread_func, this);
	if(err != 0){
		log_fatal("can't create thread: %s", strerror(err));
		exit(0);
	}
}

void SSDBImpl::stop() {
	Locking<Mutex> l(&this->mutex_bgtask_);

    this->bgtask_flag_ = false;
    std::queue<std::string> tmp_tasks_;
    tasks_.swap(tmp_tasks_);
}

void SSDBImpl::load_delete_keys_from_db(int num) {
    std::string start;
    start.append(1, DataType::DELETE);
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
    DeleteKey dk;
    if(dk.DecodeDeleteKey(del_key) == -1){
        log_fatal("delete key error! %s", hexstr(del_key).c_str());
        return;
    }

    log_debug("deleting key %s , version %d " , hexstr(dk.key).c_str() , dk.version);
//    char log_type=BinlogType::SYNC;
    std::string start = encode_hash_key(dk.key, "", dk.version);
    std::string z_start = encode_zscore_prefix(dk.key, dk.version);


    auto it = std::unique_ptr<Iterator>(this->iterator(start, "", -1));
	leveldb::WriteBatch batch;
    while (it->next()){
		if (it->key().empty() || it->key().data()[0] != DataType::ITEM){
			break;
		}

		ItemKey ik;
        std::string item_key = it->key().String();
        if (ik.DecodeItemKey(item_key) == -1){
            log_fatal("decode delete key error! %s" , hexmem(item_key.data(), item_key.size()).c_str());
            break;
        }
        if (ik.key == dk.key && ik.version == dk.version){
			batch.Delete(item_key);
        } else{
            break;
        }
    }

	//clean z*
	auto zit = std::unique_ptr<Iterator>(this->iterator(z_start, "", -1));
    while (zit->next()){
		if (zit->key().empty() || zit->key().data()[0] != DataType::ZSCORE){
			break;
		}

		ZScoreItemKey zk;
		std::string item_key = zit->key().String();
        if (zk.DecodeItemKey(item_key) == -1){
            log_fatal("decode delete key error! %s" , hexmem(item_key.data(), item_key.size()).c_str());
            break;
        }
        if (zk.key == dk.key && zk.version == dk.version){
			batch.Delete(item_key);
        } else{
            break;
        }
    }

	batch.Delete(del_key);
    RecordLock<Mutex> l(&mutex_record_, dk.key);
    if (delete_meta_key(dk, batch) == -1){
        log_fatal("delete meta key error! %s", hexstr(del_key).c_str());
        return;
    }

	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Write(write_opts, &batch);
    if (!s.ok()){
        log_fatal("SSDBImpl::delKey Backend Task error! %s", hexstr(del_key).c_str());
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
