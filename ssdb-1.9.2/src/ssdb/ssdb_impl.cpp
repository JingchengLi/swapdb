/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <util/file.h>
#include "ssdb_impl.h"

#ifdef USE_LEVELDB
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#else

#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice_transform.h"
#include <rocksdb/utilities/sim_cache.h>
#include <redis/crc/crc64speed.h>

extern "C" {
#include <redis/zmalloc.h>
};


#include "t_listener.h"

#define leveldb rocksdb
#endif

#include "redis/dump_encode.h"


SSDBImpl::SSDBImpl()  {
    ldb = NULL;
    this->bgtask_quit = false;
    expiration = NULL;
}

SSDBImpl::~SSDBImpl() {
    this->stop();

    if (expiration) {
        delete expiration;
    }


    for (auto handle : handles) {
        log_info("ColumnFamilyHandle %s finalized", handle->GetName().c_str());
        delete handle;
    }

    if (ldb) {
        log_info("DB %s finalized", ldb->GetName().c_str());
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

SSDB *SSDB::open(const Options &opt, const std::string &dir) {
    SSDBImpl *ssdb = new SSDBImpl();

    ssdb->path = dir;
    ssdb->options.create_if_missing = opt.create_if_missing;
    ssdb->options.create_missing_column_families = opt.create_missing_column_families;
    ssdb->options.max_open_files = opt.max_open_files;

#ifdef USE_LEVELDB
    ssdb->options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    ssdb->options.block_cache = leveldb::NewLRUCache(opt.cache_size * 1048576);
    ssdb->options.block_size = opt.block_size * 1024;
    ssdb->options.compaction_speed = opt.compaction_speed;
#else

    {
        //BlockBasedTableOptions
        leveldb::BlockBasedTableOptions op;
        std::shared_ptr<rocksdb::Cache> normal_block_cache = leveldb::NewLRUCache(opt.cache_size * UNIT_MB);

        if (opt.sim_cache > 0) {
            std::shared_ptr<rocksdb::Cache> sim_cache =
                    leveldb::NewSimCache(normal_block_cache, opt.sim_cache * UNIT_MB, 10);
            op.block_cache = sim_cache;
            ssdb->simCache = (leveldb::SimCache*)((sim_cache).get());
        } else {
            op.block_cache = normal_block_cache;
        }



        op.filter_policy = std::shared_ptr<const leveldb::FilterPolicy>(leveldb::NewBloomFilterPolicy(10));
        op.block_size = opt.block_size * UNIT_KB;

        ssdb->options.table_factory = std::shared_ptr<leveldb::TableFactory>(rocksdb::NewBlockBasedTableFactory(op));
    }


    ssdb->options.compaction_readahead_size = opt.compaction_readahead_size * UNIT_MB;

    ssdb->options.level0_file_num_compaction_trigger = opt.level0_file_num_compaction_trigger; //start compaction
    ssdb->options.level0_slowdown_writes_trigger = opt.level0_slowdown_writes_trigger; //slow write
    ssdb->options.level0_stop_writes_trigger = opt.level0_stop_writes_trigger;  //block write
    //========
    ssdb->options.target_file_size_base = opt.target_file_size_base * UNIT_MB; //sst file target size

//    ssdb->options.IncreaseParallelism(opt.max_background_cd_threads);

    ssdb->options.max_write_buffer_number = opt.max_write_buffer_number;


    auto env = rocksdb::Env::Default();

    ssdb->options.max_background_flushes = opt.max_background_flushes;
    env->SetBackgroundThreads(opt.max_background_flushes, rocksdb::Env::HIGH);

    ssdb->options.max_background_compactions = opt.max_background_compactions;
    env->SetBackgroundThreads(opt.max_background_compactions, rocksdb::Env::LOW);

    //level config
    ssdb->options.use_direct_reads = opt.use_direct_reads;  //see : http://rocksdb.org/blog/2015/07/23/dynamic-level.html
    ssdb->options.level_compaction_dynamic_level_bytes = opt.level_compaction_dynamic_level_bytes;  //see : http://rocksdb.org/blog/2015/07/23/dynamic-level.html
    ssdb->options.max_bytes_for_level_base = opt.max_bytes_for_level_base * UNIT_MB; //256M
    ssdb->options.max_bytes_for_level_multiplier = opt.max_bytes_for_level_multiplier; //10  // multiplier between levels

            //rate_limiter
//    ssdb->options.rate_limiter = std::shared_ptr<leveldb::GenericRateLimiter>(new leveldb::GenericRateLimiter(1024 * 50, 1000, 10));
    // refill_bytes  refill_period_us  1024, 1000 = 1MB/s

   ssdb->options.listeners.push_back(std::shared_ptr<t_listener>(new t_listener()));

#endif
    ssdb->options.write_buffer_size = static_cast<size_t >(opt.write_buffer_size) * UNIT_MB;
    if (opt.compression) {
        ssdb->options.compression = leveldb::kSnappyCompression;
    } else {
        ssdb->options.compression = leveldb::kNoCompression;
    }


    leveldb::Status status;

    // open DB with two column families
    std::vector<leveldb::ColumnFamilyDescriptor> column_families;

    column_families.emplace_back(leveldb::ColumnFamilyDescriptor(leveldb::kDefaultColumnFamilyName, ssdb->options));

    column_families.emplace_back(leveldb::ColumnFamilyDescriptor(REPOPID_CF, leveldb::ColumnFamilyOptions()));

    status = leveldb::DB::Open(ssdb->options, ssdb->getDataPath(), column_families, &ssdb->handles, &ssdb->ldb);
    if (!status.ok()) {
        log_error("open db failed: %s", status.ToString().c_str());
        delete ssdb;
        return nullptr;
    }

    ssdb->expiration = new ExpirationHandler(ssdb); //todo 后续如果支持set命令中设置过期时间，添加此行，同时删除serv.cpp中相应代码
    ssdb->start();

    return ssdb;
}

int SSDBImpl::filesize(Context &ctx, uint64_t *total_file_size) {
    if (!is_dir(getDataPath())) {
        return -1;
    }

    leveldb::Env *env = leveldb::Env::Default();
    std::vector<std::string> result;
    leveldb::Status s = env->GetChildren(getDataPath(), &result);
    if (!s.ok()){
        //error
        log_error("error: %s", s.ToString().c_str());
        return STORAGE_ERR;
    }

    for_each(result.begin(), result.end() ,[&](std::string filename) {
        if (filename== "." || filename == "..") {
            //ignore
        }
        uint64_t file_size = 0;
        s = env->GetFileSize(getDataPath() + filename, &file_size);
        if (!s.ok()){
            //error
            log_error("error: %s", s.ToString().c_str());
        } else {
//            log_debug("%s file size : %d", filename.c_str(), file_size);
            (*total_file_size) += file_size;
        }
    });

    return (int) result.size();
}

int SSDBImpl::flush(Context &ctx, bool wait) {

    leveldb::FlushOptions flushOptions;

    flushOptions.wait = wait;

    ldb->Flush(flushOptions);

    return 0;
}

int SSDBImpl::flushdb(Context &ctx) {
//lock

    Locking<RecordKeyMutex> gl(&mutex_record_);
    redisCursorService.ClearAllCursor();

#ifdef USE_LEVELDB

#else
    log_info("[flushdb] using DeleteFilesInRange");
    leveldb::Slice begin("0");
    leveldb::Slice end("z");
    leveldb::DeleteFilesInRange(ldb, ldb->DefaultColumnFamily(), &begin, &end);

    if (ROCKSDB_MAJOR >= 5) {
        log_info("[flushdb] using DeleteRange");

        ldb->DeleteRange(leveldb::WriteOptions(), ldb->DefaultColumnFamily(), begin, end);
        ldb->Flush(leveldb::FlushOptions(), ldb->DefaultColumnFamily());
    }
#endif


    uint64_t total = 0;
    int ret = 1;
    bool stop = false;

    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    leveldb::WriteOptions write_opts;

    unique_ptr<leveldb::Iterator> it = unique_ptr<leveldb::Iterator>(ldb->NewIterator(iterate_options, ldb->DefaultColumnFamily()));

    it->SeekToFirst();

    while (!stop) {

        leveldb::WriteBatch writeBatch;

        for (int i = 0; i < 100000; i++) {
            if (!it->Valid()) {
                stop = true;
                break;
            }

            total++;
            writeBatch.Delete(it->key());
            it->Next();
        }

        leveldb::Status s = ldb->Write(write_opts, &writeBatch);
        if (!s.ok()) {
            log_error("del error: %s", s.ToString().c_str());
            stop = true;
            ret = -1;
        }

    }

    leveldb::WriteBatch writeBatch;
    leveldb::Status s = CommitBatch(ctx, write_opts, &writeBatch );
    if (!s.ok()) {
        ret = -1;
    }

    log_info("[flushdb] %d keys deleted by iteration", total);

    return ret;
}

Iterator *SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit,
                             const leveldb::Snapshot *snapshot) {
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

Iterator *SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit,
                              const leveldb::ReadOptions& iterate_options) {
    leveldb::Iterator *it;
    it = ldb->NewIterator(iterate_options);
    it->Seek(start);
    return new Iterator(it, end, limit, Iterator::FORWARD, iterate_options.snapshot);
}

Iterator *SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit,
                                 const leveldb::Snapshot *snapshot) {
    leveldb::Iterator *it;
    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    if (snapshot) {
        iterate_options.snapshot = snapshot;
    }
    it = ldb->NewIterator(iterate_options);
    it->Seek(start);
    if (!it->Valid()) {
        it->SeekToLast();
    } else {
        if (memcmp(start.data(), it->key().data(), start.size()) != 0) {
            it->Prev();
        }
    }
    return new Iterator(it, end, limit, Iterator::BACKWARD, iterate_options.snapshot);
}

const leveldb::Snapshot *SSDBImpl::GetSnapshot() {
    if (ldb) {
        return ldb->GetSnapshot();
    }
    return nullptr;
}

void SSDBImpl::ReleaseSnapshot(const leveldb::Snapshot *snapshot = nullptr) {
    if (snapshot != nullptr) {
        ldb->ReleaseSnapshot(snapshot);
    }
}

/* raw operates */

int SSDBImpl::raw_set(Context &ctx, const Bytes &key, const Bytes &val) {
    leveldb::WriteOptions write_opts;
    leveldb::Status s = ldb->Put(write_opts, slice(key), slice(val));
    if (!s.ok()) {
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::raw_del(Context &ctx, const Bytes &key) {
    leveldb::WriteOptions write_opts;
    leveldb::Status s = ldb->Delete(write_opts, slice(key));
    if (!s.ok()) {
        log_error("del error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::raw_get(Context &ctx, const Bytes &key, std::string *val) {
    return raw_get(ctx, key, handles[0], val);
}

int SSDBImpl::raw_get(Context &ctx, const Bytes &key, leveldb::ColumnFamilyHandle *column_family, std::string *val) {
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    leveldb::Status s = ldb->Get(opts, column_family, slice(key), val);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

uint64_t SSDBImpl::size() {
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

std::vector<std::string> SSDBImpl::info() {
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
     for (int i = 0; i < 7; i++) {
        keys.push_back(leveldb::DB::Properties::kNumFilesAtLevelPrefix + str(i));
//        keys.push_back(leveldb::DB::Properties::kCompressionRatioAtLevelPrefix + str(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        std::string key = keys[i];
        std::string val;
        if (ldb->GetProperty(key, &val)) {
            info.push_back(key + " : " + val);
        }
    }

    keys.clear();

    keys.push_back(leveldb::DB::Properties::kStats);

    keys.push_back(leveldb::DB::Properties::kSSTables);
    keys.push_back(leveldb::DB::Properties::kLevelStats);
    keys.push_back(leveldb::DB::Properties::kNumSnapshots);
    keys.push_back(leveldb::DB::Properties::kOldestSnapshotTime);
    keys.push_back(leveldb::DB::Properties::kTotalSstFilesSize);
    keys.push_back(leveldb::DB::Properties::kEstimateLiveDataSize);

    keys.push_back(leveldb::DB::Properties::kEstimateTableReadersMem);
    keys.push_back(leveldb::DB::Properties::kCurSizeAllMemTables);

#endif

    for (size_t i = 0; i < keys.size(); i++) {
        std::string key = keys[i];
        std::string val;
        if (ldb->GetProperty(key, &val)) {
            info.push_back(key);
            info.push_back(val);

        }
    }

    info.push_back("");


    return info;
}

void SSDBImpl::compact() {
#ifdef USE_LEVELDB
    ldb->CompactRange(NULL, NULL);
#else
    leveldb::CompactRangeOptions compactRangeOptions = rocksdb::CompactRangeOptions();
    ldb->CompactRange(compactRangeOptions, NULL, NULL);
#endif
}


leveldb::Status
SSDBImpl::CommitBatch(Context &ctx, const leveldb::WriteOptions &options, leveldb::WriteBatch *updates) {

    if (ctx.replLink && ctx.isFirstbatch()) {

        if (ctx.currentSeqCnx < ctx.lastSeqCnx) {
            log_error("ctx.currentSeqCnx[%s] < ctx.lastSeqCnx[%s]",
                      ctx.currentSeqCnx.toString().c_str(),
                      ctx.lastSeqCnx.toString().c_str());
            assert(0);
        }

//        if ((ctx.currentSeqCnx.timestamp == ctx.lastSeqCnx.timestamp)) {
//            if (ctx.currentSeqCnx != 0) {
//
//                int64_t res = ctx.currentSeqCnx.id - ctx.lastSeqCnx.id;
//
//                if (res != 1) {
//                    log_error("ctx.currentSeqCnx.id(%d) - ctx.lastSeqCnx.id(%d) != 1", ctx.currentSeqCnx.id,
//                              ctx.lastSeqCnx.id);
//                }
//            }
//
//        } else {
//            if (ctx.currentSeqCnx != 1) {
//                log_error("ctx.currentSeqCnx.id(%d) != 1", ctx.currentSeqCnx.id);
//            }
//
//        }

        updates->Put(handles[1], encode_repo_key(),
                     encode_repo_item(ctx.currentSeqCnx.timestamp, ctx.currentSeqCnx.id));

    }
    leveldb::Status s = ldb->Write(options, updates);

    if (ctx.replLink) {
        ctx.lastSeqCnx = ctx.currentSeqCnx;
        ctx.setFirstbatch(false);
    }

    return s;
}

leveldb::Status SSDBImpl::CommitBatch(Context &ctx, leveldb::WriteBatch *updates) {

    return CommitBatch(ctx, leveldb::WriteOptions(), updates);

}


void SSDBImpl::start() {
    this->bgtask_quit = false;
    int err = pthread_create(&bg_tid_, NULL, &thread_func, this);
    if (err != 0) {
        log_fatal("can't create thread: %s", strerror(err));
        exit(0);
    }
}

void SSDBImpl::stop() {
    Locking<Mutex> l(&this->mutex_bgtask_);
    log_info("del thread stopping");

    this->bgtask_quit = true;
    for (int i = 0; i < 1000; i++) {
        if (!bgtask_quit) {
            break;
        }
        log_info("waiting for del thread stop");
        usleep(1000 * 1000);
    }

    std::queue<std::string> tmp_tasks_;
    tasks_.swap(tmp_tasks_);
}

void SSDBImpl::load_delete_keys_from_db(int num) {
    std::string start;
    start.append(1, DataType::DELETE);
    auto it = std::unique_ptr<Iterator>(this->iterator(start, "", num));
    while (it->next()) {
        if (it->key().String()[0] != DataType::DELETE) {
            break;
        }
        tasks_.push(it->key().String());
    }
}

int SSDBImpl::delete_meta_key(const DeleteKey &dk, leveldb::WriteBatch &batch) {
    std::string meta_key = encode_meta_key(dk.key);
    std::string meta_val;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), meta_key, &meta_val);
    if (!s.ok() && !s.IsNotFound()) {
        return -1;
    } else if (s.ok()) {
        Decoder decoder(meta_val.data(), meta_val.size());
        if (decoder.skip(1) == -1) {
            return -1;
        }

        uint16_t version = 0;
        if (decoder.read_uint16(&version) == -1) {
            return -1;
        } else {
            version = be16toh(version);
        }

        char del = meta_val[3];

        if (del == KEY_DELETE_MASK && version == dk.version) {
            batch.Delete(meta_key);
        }
    }
    return 0;
}

void SSDBImpl::delete_key_loop(const std::string &del_key) {
    DeleteKey dk;
    if (dk.DecodeDeleteKey(del_key) == -1) {
        log_fatal("delete key error! %s", hexstr(del_key).c_str());
        return;
    }

    log_debug("deleting key %s , version %d ", hexstr(dk.key).c_str(), dk.version);
//    char log_type=BinlogType::SYNC;
    std::string start = encode_hash_key(dk.key, "", dk.version);
    std::string z_start = encode_zscore_prefix(dk.key, dk.version);


    auto it = std::unique_ptr<Iterator>(this->iterator(start, "", -1));
    leveldb::WriteBatch batch;
    while (it->next()) {
        if (it->key().empty() || it->key().data()[0] != DataType::ITEM) {
            break;
        }

        ItemKey ik;
        Bytes item_key = it->key();
        if (ik.DecodeItemKey(item_key) == -1) {
            log_fatal("decode delete key error! %s", hexstr(item_key).c_str());
            break;
        }
        if (ik.key == dk.key && ik.version == dk.version) {
            batch.Delete(slice(item_key));
        } else {
            break;
        }
    }

    //clean z*
    auto zit = std::unique_ptr<Iterator>(this->iterator(z_start, "", -1));
    while (zit->next()) {
        if (zit->key().empty() || zit->key().data()[0] != DataType::ZSCORE) {
            break;
        }

        ZScoreItemKey zk;
        Bytes item_key = zit->key();
        if (zk.DecodeItemKey(item_key) == -1) {
            log_fatal("decode delete key error! %s", hexstr(item_key).c_str());
            break;
        }
        if (zk.key == dk.key && zk.version == dk.version) {
            batch.Delete(slice(item_key));
        } else {
            break;
        }
    }

    batch.Delete(del_key);
    RecordKeyLock l(&mutex_record_, dk.key);
    if (delete_meta_key(dk, batch) == -1) {
        log_fatal("delete meta key error! %s", hexstr(del_key).c_str());
        return;
    }

    leveldb::WriteOptions write_opts;
    write_opts.disableWAL = true;
    leveldb::Status s = ldb->Write(write_opts, &batch);
    if (!s.ok()) {
        log_fatal("SSDBImpl::delKey Backend Task error! %s", hexstr(del_key).c_str());
        return;
    }
}

void SSDBImpl::runBGTask() {
    while (!bgtask_quit) {
        mutex_bgtask_.lock();
        if (tasks_.empty()) {
            load_delete_keys_from_db(1000);
            if (tasks_.empty()) {
                mutex_bgtask_.unlock();
                usleep(1000 * 1000);
                continue;
            }
        }

        std::string del_key;
        if (!tasks_.empty()) {
            del_key = tasks_.front();
            tasks_.pop();
        }
        mutex_bgtask_.unlock();

        if (!del_key.empty()) {
            delete_key_loop(del_key);
            sched_yield();
        }
    }

    bgtask_quit = false;
}

void *SSDBImpl::thread_func(void *arg) {
    SSDBImpl *bg_task = (SSDBImpl *) arg;

    bg_task->runBGTask();

    return (void *) NULL;
}


#include "rocksdb/utilities/backupable_db.h"


class RocksdbWritableFileEncoder : public RedisEncoder {
public:
    explicit RocksdbWritableFileEncoder(rocksdb::WritableFile *handle) : handle(handle) {}

    leveldb::WritableFile *handle;
    leveldb::Status s;

    uint64_t cksum;

    bool update_cksum = false;

    void genericUpdateChecksum(void *p, size_t n) {
        cksum = crc64(cksum, p, n);
    }

    int rdbWriteRaw(void *p, size_t n) override {
        if (handle != nullptr) {
            s = handle->Append(leveldb::Slice((const char *) p, n));
            if (update_cksum) {
                genericUpdateChecksum(p , n);
            }
            if (!s.ok()) {
                return -1;
            }
        }
        return 0;
    }
};

int SSDBImpl::save(Context &ctx) {
    Locking<Mutex> l(&this->mutex_backup_);

    rocksdb::Status s;
    rocksdb::DB *db = ldb;

    int64_t now = time_ms();

    leveldb::EnvOptions options;
    unique_ptr<leveldb::WritableFile> saved;
    leveldb::Env *env = leveldb::Env::Default();
    s = env->NewWritableFile(path + "/rdb.dump", &saved, options);

    if (!s.ok()) {
        log_error("%s", s.ToString().c_str());
        return -1;
    }

    if (!saved) {
        return -1;
    }

    RocksdbWritableFileEncoder encoder(saved.get());
    encoder.update_cksum = true;

    char magic[10];
    snprintf(magic, sizeof(magic), "REDIS%04d", FAKE_RDB_VERSION);
    if (encoder.rdbWriteRaw(magic, 9) == -1) return -1;

    /* Add a few fields about the state when the RDB was created. */
    int redis_bits = (sizeof(void *) == 8) ? 64 : 32;

    if (encoder.rdbSaveAuxFieldStrStr("redis-ver", FAKE_REDIS_VERSION) == -1) return -1;
    if (encoder.rdbSaveAuxFieldStrInt("redis-bits", redis_bits) == -1) return -1;
    if (encoder.rdbSaveAuxFieldStrInt("ctime", time(NULL)) == -1) return -1;
    if (encoder.rdbSaveAuxFieldStrInt("used-mem", (long long int) zmalloc_get_rss()) == -1) return -1;


    if (encoder.rdbSaveType(RDB_OPCODE_SELECTDB) == -1) return -1;
    if (encoder.rdbSaveLen(0) == -1) return -1;

    if (encoder.rdbSaveType(RDB_OPCODE_RESIZEDB) == -1)return -1;
    if (encoder.rdbSaveLen(UINT32_MAX) == -1) return -1;
    if (encoder.rdbSaveLen(UINT32_MAX) == -1) return -1;


    std::string start;
    start.append(1, DataType::META);

    char dtype;

    const leveldb::Snapshot *snapshot = GetSnapshot();
    SnapshotPtr spl(ldb, snapshot); //auto release


    auto it = std::unique_ptr<MIterator>(new MIterator(iterator(start, "", -1, snapshot)));
    while(it->next()){
        const Bytes& key = it->key;
        const std::string& meta_val = it->val.String();

        //decodeMetaVal
        if(meta_val.size()<4) {
            //invalid
            log_error("invalid MetaVal: %s", s.ToString().c_str());
            continue;
        }

        char del = meta_val[POS_DEL];
        if (del != KEY_ENABLED_MASK){
            //deleted
            continue;
        }

        int64_t expire = expiration->pttl(ctx, key, TimeUnit::Millisecond);
        if (expire == -2) continue;

        if (expire != -1) {
            /* If this key is already expired skip it */
            if (encoder.rdbSaveType(RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
            if (encoder.rdbSaveMillisecondTime(expire) == -1) return -1;
        }

        //save type
        dtype = meta_val[0];
        if (encoder.rdbSaveObjectType(dtype) == -1) return -1;
        if (encoder.rdbSaveRawString(key.String()) == -1) return -1;
        if (rdbSaveObject(ctx, key, dtype, meta_val, encoder, snapshot) < 0) return -1;

    }


    if (encoder.rdbSaveType(RDB_OPCODE_EOF) == -1) return -1;

    uint64_t cksum = encoder.cksum;
    memrev64ifbe(&cksum);
    if (encoder.saveRawString(Bytes(&cksum,8)) == -1) return -1;

    return 0;
}
