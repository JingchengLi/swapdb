#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

#include <queue>
#include <list>
#include <map>
#include <atomic>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"

#include "nemo_options.h"
#include "nemo_const.h"
#include "nemo_iterator.h"
#include "nemo_meta.h"
#include "port.h"
#include "util.h"
#include "xdebug.h"

namespace nemo {

typedef rocksdb::Status Status;
typedef const rocksdb::Snapshot Snapshot;
typedef std::vector<const rocksdb::Snapshot *> Snapshots;

template <typename T1, typename T2>
struct ItemListMap{
    int64_t cur_size_;
    int64_t max_size_;
    std::list<T1> list_;
    std::map<T1, T2> map_;
};

/*
 * There are two type OP: DEL_KEY and CLEAN_RANGE;
 * DEL_KEY use only the first parameter argv1;
 * CLEAN_RANGE will compact the range [argv1, argv2];
 */
struct BGTask {
  DBType     type;
  OPERATION   op;
  std::string argv1;
  std::string argv2;

  BGTask() : type(DBType::kNONE_DB), op(OPERATION::kNONE_OP) { }
  BGTask(const DBType _type, const OPERATION _op, const std::string &_argv1, const std::string &_argv2)
      : type(_type), op(_op), argv1(_argv1), argv2(_argv2) {}
};
class Nemo {
public:
    Nemo(const std::string &db_path, const Options &options);
    ~Nemo() {
        bgtask_flag_ = false;
        bg_cv_.Signal();
        int ret = 0;
        if ((ret = pthread_join(bg_tid_, NULL)) != 0) {
          log_warn("pthread_join failed with bgtask thread error %d", ret);
        }

        kv_db_.reset();
        hash_db_.reset();
        list_db_.reset();
        zset_db_.reset();
        set_db_.reset();
        //delete kv_db_.get();
        //delete hash_db_.get();
        //delete list_db_.get();
        //delete zset_db_.get();
        //delete set_db_.get();

        pthread_mutex_destroy(&(mutex_cursors_));
        pthread_mutex_destroy(&(mutex_dump_));
        //pthread_mutex_destroy(&(mutex_bgtask_));
    };

    // Used for pika
    Status Compact(DBType type, bool sync = false);
    Status RunBGTask();
    std::string GetCurrentTaskType();


    // =================String=====================
    Status Del(const std::string &key, int64_t *count);
    Status MDel(const std::vector<std::string> &keys, int64_t* count);
    Status Expire(const std::string &key, const int32_t seconds, int64_t *res);
    Status TTL(const std::string &key, int64_t *res);
    Status Persist(const std::string &key, int64_t *res);
    Status Expireat(const std::string &key, const int32_t timestamp, int64_t *res);
    Status Type(const std::string &key, std::string* type);
    Status Exists(const std::vector<std::string> &key, int64_t* res);

    // =================KV=====================
    Status Set(const std::string &key, const std::string &val, const int32_t ttl = 0);
    Status Get(const std::string &key, std::string *val);
    Status MSet(const std::vector<KV> &kvs);
    Status KMDel(const std::vector<std::string> &keys, int64_t* count);
    Status MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss);
    Status Incrby(const std::string &key, const int64_t by, std::string &new_val);
    Status Decrby(const std::string &key, const int64_t by, std::string &new_val);
    Status Incrbyfloat(const std::string &key, const double by, std::string &new_val);
    Status GetSet(const std::string &key, const std::string &new_val, std::string *old_val);
    Status Append(const std::string &key, const std::string &value, int64_t *new_len);
    Status Setnx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl = 0);
    Status Setxx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl = 0);
    Status MSetnx(const std::vector<KV> &kvs, int64_t *ret);
    Status Getrange(const std::string key, const int64_t start, const int64_t end, std::string &substr);
    Status Setrange(const std::string key, const int64_t offset, const std::string &value, int64_t *len);
    Status Strlen(const std::string &key, int64_t *len);
    KIterator* KScan(const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot = false);
    Status Scan(int64_t cursor, std::string &pattern, int64_t count, std::vector<std::string>& keys, int64_t* cursor_ret);

    Status Keys(const std::string &pattern, std::vector<std::string>& keys);

    // ==============BITMAP=====================
    //TODO INT* instead of int&
    Status BitSet(const std::string &key, const std::int64_t offset, const std::int64_t on, std::int64_t* res);
    Status BitGet(const std::string &key, const std::int64_t offset, std::int64_t* res);
    Status BitCount(const std::string &key, std::int64_t* res);
    Status BitCount(const std::string &key, std::int64_t start_offset, std::int64_t end_offset, std::int64_t* res);
    Status BitPos(const std::string &key, const int64_t bit, std::int64_t* res);
    Status BitPos(const std::string &key, const int64_t bit, const std::int64_t start_offset, std::int64_t* res);
    Status BitPos(const std::string &key, const int64_t bit, const std::int64_t start_offset, const std::int64_t end_offset, std::int64_t* res);
    Status BitOp(BitOpType op, const std::string &dest_key, const std::vector<std::string>& src_keys, int64_t* result_length);

    // used only for bada_kv
    Status SetWithExpireAt(const std::string &key, const std::string &val, const int32_t timestamp = 0);

    // ==============HASH=====================
    Status HSet(const std::string &key, const std::string &field, const std::string &val);
    Status HGet(const std::string &key, const std::string &field, std::string *val);
    Status HDel(const std::string &key, const std::string &field);
    bool HExists(const std::string &key, const std::string &field);
    Status HKeys(const std::string &key, std::vector<std::string> &keys);
    Status HGetall(const std::string &key, std::vector<FV> &fvs);
    int64_t HLen(const std::string &key);
    Status HMSet(const std::string &key, const std::vector<FV> &fvs);
    Status HMGet(const std::string &key, const std::vector<std::string> &keys, std::vector<FVS> &fvss);
    Status HSetnx(const std::string &key, const std::string &field, const std::string &val);
    int64_t HStrlen(const std::string &key, const std::string &field);
    HIterator* HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot = false);
    Status HVals(const std::string &key, std::vector<std::string> &vals);
    Status HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val);
    Status HIncrbyfloat(const std::string &key, const std::string &field, double by, std::string &new_val);
    
    // ==============List=====================
    Status LIndex(const std::string &key, const int64_t index, std::string *val);
    Status LLen(const std::string &key, int64_t *llen);
    Status LPush(const std::string &key, const std::string &val, int64_t *llen);
    Status LPop(const std::string &key, std::string *val);
    Status LPushx(const std::string &key, const std::string &val, int64_t *llen);
    Status LRange(const std::string &key, const int64_t begin, const int64_t end, std::vector<IV> &ivs);
    Status LSet(const std::string &key, const int64_t index, const std::string &val);
    Status LTrim(const std::string &key, const int64_t begin, const int64_t end);
    Status RPush(const std::string &key, const std::string &val, int64_t *llen);
    Status RPop(const std::string &key, std::string *val);
    Status RPushx(const std::string &key, const std::string &val, int64_t *llen);
    Status RPopLPush(const std::string &src, const std::string &dest, std::string &val);
    Status LInsert(const std::string &key, Position pos, const std::string &pivot, const std::string &val, int64_t *llen);
    Status LRem(const std::string &key, const int64_t count, const std::string &val, int64_t *rem_count);

    // ==============ZSet=====================
    Status ZAdd(const std::string &key, const double score, const std::string &member, int64_t *res);
    int64_t ZCard(const std::string &key);
    int64_t ZCount(const std::string &key, const double begin, const double end, bool is_lo = false, bool is_ro = false);
    ZIterator* ZScan(const std::string &key, const double begin, const double end, uint64_t limit, bool use_snapshot = false);
    Status ZIncrby(const std::string &key, const std::string &member, const double by, std::string &new_val);

    Status ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms);
    Status ZUnionStore(const std::string &destination, const int numkeys, const std::vector<std::string> &keys, const std::vector<double> &weights, Aggregate agg, int64_t *res);
    Status ZInterStore(const std::string &destination, const int numkeys, const std::vector<std::string> &keys, const std::vector<double> &weights, Aggregate agg, int64_t *res);
    Status ZRangebyscore(const std::string &key, const double start, const double stop, std::vector<SM> &sms, bool is_lo = false, bool is_ro = false);
    Status ZRem(const std::string &key, const std::string &member, int64_t *res);
    Status ZRank(const std::string &key, const std::string &member, int64_t *rank);
    Status ZRevrank(const std::string &key, const std::string &member, int64_t *rank);
    Status ZScore(const std::string &key, const std::string &member, double *score);
    Status ZRangebylex(const std::string &key, const std::string &min, const std::string &max, std::vector<std::string> &members);
    Status ZLexcount(const std::string &key, const std::string &min, const std::string &max, int64_t* count);
    Status ZRemrangebylex(const std::string &key, const std::string &min, const std::string &max, bool is_lo, bool is_ro, int64_t* count);
    Status ZRemrangebyrank(const std::string &key, const int64_t start, const int64_t stop, int64_t* count);
    Status ZRemrangebyscore(const std::string &key, const double start, const double stop, int64_t* count, bool is_lo = false, bool is_ro = false);

    // ==============Set=====================
    Status SAdd(const std::string &key, const std::string &member, int64_t *res);
    Status SRem(const std::string &key, const std::string &member, int64_t *res);
    int64_t SCard(const std::string &key);
    SIterator* SScan(const std::string &key, uint64_t limit, bool use_snapshot = false);
    Status SMembers(const std::string &key, std::vector<std::string> &vals);
    Status SUnionStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res);
    Status SUnion(const std::vector<std::string> &keys, std::vector<std::string>& members);
    Status SInterStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res);
    Status SInter(const std::vector<std::string> &keys, std::vector<std::string>& members);
    Status SDiffStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res);
    Status SDiff(const std::vector<std::string> &keys, std::vector<std::string>& members);
    bool SIsMember(const std::string &key, const std::string &member);
    Status SPop(const std::string &key, std::string &member);
    Status SRandMember(const std::string &key, std::vector<std::string> &members, const int count = 1);
    Status SMove(const std::string &source, const std::string &destination, const std::string &member, int64_t *res);

    // ==============Server=====================
    Status BGSave(Snapshots &snapshots, const std::string &db_path = ""); 
    Status BGSaveGetSnapshot(Snapshots &snapshots);
    Status BGSaveSpecify(const std::string key_type, Snapshot* snapshot);
    Status BGSaveGetSpecifySnapshot(const std::string key_type, Snapshot *&snapshot);
    Status BGSaveOff();
    //Status BGSaveReleaseSnapshot(Snapshots &snapshots);

    Status GetKeyNum(std::vector<uint64_t> &nums);
    Status GetSpecifyKeyNum(const std::string type, uint64_t &num);
    //Status ScanKeyNum(std::unique_ptr<rocksdb::DB> &db, const char kType, uint64_t &num);
    Status ScanKeyNum(std::unique_ptr<rocksdb::DBWithTTL> &db, const char kType, uint64_t &num);
    Status ScanKeyNumWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, uint64_t &num);
    Status StopScanKeyNum();
    
    Status GetUsage(const std::string& type, uint64_t *result);

    rocksdb::DBWithTTL* GetDBByType(const std::string& type); 
    
    /* Meta */
    // Scan all metas of db specified by given type
    Status ScanMetasSpecify(DBType type, const std::string &pattern,
        std::map<std::string, MetaPtr>& metas);
    // Check and recover data
    Status CheckMetaSpecify(DBType type, const std::string &pattern);
    // ChecknRecover function for different type db
    Status ChecknRecover(DBType type, const std::string& key);
    Status HChecknRecover(const std::string& key);
    Status LChecknRecover(const std::string& key);
    Status SChecknRecover(const std::string& key);
    Status ZChecknRecover(const std::string& key);

private:

    std::string db_path_;
    rocksdb::Options open_options_;
    std::unique_ptr<rocksdb::DBWithTTL> kv_db_;
    std::unique_ptr<rocksdb::DBWithTTL> hash_db_;
    //std::unique_ptr<rocksdb::DB> hash_db_;
    std::unique_ptr<rocksdb::DBWithTTL> list_db_;
    std::unique_ptr<rocksdb::DBWithTTL> zset_db_;
    std::unique_ptr<rocksdb::DBWithTTL> set_db_;

    port::RecordMutex mutex_hash_record_;
    port::RecordMutex mutex_kv_record_;
    port::RecordMutex mutex_list_record_;
    port::RecordMutex mutex_zset_record_;
    port::RecordMutex mutex_set_record_;

    bool save_flag_;

    //pthread_mutex_t mutex_bgtask_;
    port::Mutex mutex_bgtask_;
    std::atomic<bool> bgtask_flag_;

    // Maybe 0 for none, 1 for compact_key, and 2 for compact all;
    std::atomic<int> current_task_type_;
    pthread_t bg_tid_;
    std::queue<BGTask> bg_tasks_;
    port::CondVar bg_cv_;

    // Used for compact tools and internal
    Status DoCompact(DBType type);
    Status AddBGTask(const BGTask& task);
    Status CompactKey(const DBType type, const rocksdb::Slice& key);
    Status StartBGThread();

    Status ExistsSingleKey(const std::string &key);

    Status KDel(const std::string &key, int64_t *res);
    Status KExpire(const std::string &key, const int32_t seconds, int64_t *res);
    Status KTTL(const std::string &key, int64_t *res);
    Status KPersist(const std::string &key, int64_t *res);
    Status KExpireat(const std::string &key, const int32_t timestamp, int64_t *res);
    Status HDelKey(const std::string &key, int64_t *res);
    Status HExpire(const std::string &key, const int32_t seconds, int64_t *res);
    Status HTTL(const std::string &key, int64_t *res);
    Status HPersist(const std::string &key, int64_t *res);
    Status HExpireat(const std::string &key, const int32_t timestamp, int64_t *res);
    Status ZDelKey(const std::string &key, int64_t *res);
    Status ZExpire(const std::string &key, const int32_t seconds, int64_t *res);
    Status ZTTL(const std::string &key, int64_t *res);
    Status ZPersist(const std::string &key, int64_t *res);
    Status ZExpireat(const std::string &key, const int32_t timestamp, int64_t *res);
    Status SDelKey(const std::string &key, int64_t *res);
    Status SExpire(const std::string &key, const int32_t seconds, int64_t *res);
    Status STTL(const std::string &key, int64_t *res);
    Status SPersist(const std::string &key, int64_t *res);
    Status SExpireat(const std::string &key, const int32_t timestamp, int64_t *res);
    Status LDelKey(const std::string &key, int64_t *res);
    Status LExpire(const std::string &key, const int32_t seconds, int64_t *res);
    Status LTTL(const std::string &key, int64_t *res);
    Status LPersist(const std::string &key, int64_t *res);
    Status LExpireat(const std::string &key, const int32_t timestamp, int64_t *res);

    pthread_mutex_t mutex_cursors_;
    ItemListMap<int64_t, std::string> cursors_store_;

    pthread_mutex_t mutex_spop_counts_;
    ItemListMap<std::string, int64_t> spop_counts_store_; 
    
    int64_t AddAndGetSpopCount(const std::string &key);
    void ResetSpopCount(const std::string &key);

    Status GetSnapshot(Snapshots &snapshots);
    Status ScanKeysWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const std::string pattern, std::vector<std::string>& keys);
    bool ScanKeysWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, std::string &start_key, const std::string &pattern, std::vector<std::string>& keys, int64_t* count, std::string* next_key);
    // Remeber the snapshot will be release inside!!
    Status ScanKeys(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const char kType, const std::string &pattern, std::vector<std::string>& keys);
    bool ScanKeys(std::unique_ptr<rocksdb::DBWithTTL> &db, const char kType, std::string &start_key, const std::string &pattern, std::vector<std::string>& keys, int64_t* count, std::string* next_key);
    Status GetStartKey(int64_t cursor, std::string* start_key);
    int64_t StoreAndGetCursor(int64_t cursor, const std::string& next_key);
    Status SeekCursor(int64_t cursor, std::string* start_key);

    int DoHSet(const std::string &key, const std::string &field, const std::string &val, rocksdb::WriteBatch &writebatch);
    int DoHDel(const std::string &key, const std::string &field, rocksdb::WriteBatch &writebatch);
    Status HSetNoLock(const std::string &key, const std::string &field, const std::string &val);
    int IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    Status ZAddNoLock(const std::string &key, const double score, const std::string &member, int64_t *res);
    Status ZRemrangebyrankNoLock(const std::string &key, const int64_t start, const int64_t stop, int64_t* count);
    ZLexIterator* ZScanbylex(const std::string &key, const std::string &min, const std::string &max, uint64_t limit, bool use_snapshot = false);
    int DoZSet(const std::string &key, const double score, const std::string &member, rocksdb::WriteBatch &writebatch);
    int32_t L2R(const std::string &key, const int64_t index, const int64_t left, int64_t *priv, int64_t *cur, int64_t *next);
    int32_t R2L(const std::string &key, const int64_t index, const int64_t right, int64_t *priv, int64_t *cur, int64_t *next);

    Status RPopLPushInternal(const std::string &src, const std::string &dest, std::string &val);

    int IncrZLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    int IncrSSize(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);


    Status SAddNoLock(const std::string &key, const std::string &member, int64_t *res);
    Status SRemNoLock(const std::string &key, const std::string &member, int64_t *res);

    Status SaveDBWithTTL(const std::string &db_path, const std::string &key_type, const char meta_prefix, std::unique_ptr<rocksdb::DBWithTTL> &src_db, const rocksdb::Snapshot *snapshot);
    //Status SaveDBWithTTL(const std::string &db_path, const std::string &key_type, std::unique_ptr<rocksdb::DBWithTTL> &src_db, const rocksdb::Snapshot *snapshot);
    Status SaveDB(const std::string &db_path, std::unique_ptr<rocksdb::DB> &src_db, const rocksdb::Snapshot *snapshot);

    /* Meta */
    char GetMetaPrefix(DBType type);

    // GetProperty of 5 DBs
    uint64_t GetProperty(const std::string &property);
    // Get estimate RecordMutex memory usage
    uint64_t GetLockUsage();

    // Scan metas on given db
    Status ScanDBMetas(std::unique_ptr<rocksdb::DBWithTTL> &db, DBType type,
        const std::string &pattern, std::map<std::string, MetaPtr>& metas);
    // Scan metas on given db and given snapshot
    Status ScanDBMetasOnSnap(std::unique_ptr<rocksdb::DBWithTTL> &db, const rocksdb::Snapshot* psnap,
        DBType type, const std::string &pattern, std::map<std::string, MetaPtr>& metas);
    Status GetByKey(DBType type, const std::string &key, MetaPtr& meta);
    // Check and recover data on spcified db
    Status CheckDBMeta(std::unique_ptr<rocksdb::DBWithTTL> &db, DBType type, const std::string& pattern);
    // Get meta by key
    Status HGetMetaByKey(const std::string &key, HashMeta& meta);
    Status LGetMetaByKey(const std::string &key, ListMeta& meta);
    Status SGetMetaByKey(const std::string &key, SetMeta& meta);
    Status ZGetMetaByKey(const std::string &key, ZSetMeta& meta);

    Status ZDressZScoreforZSet(const std::string& key, int* count);
    Status ZDressZSetforZScore(const std::string& key, int* count);

    std::tuple<int64_t, int64_t> BitOpGetSrcValue(const std::vector<std::string> &src_keys, std::vector<std::string> &src_values);
    std::string BitOpOperate(BitOpType op, const std::vector<std::string> &src_values, int64_t max_len, int64_t min_len);


    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

    std::atomic<bool> scan_keynum_exit_;

    pthread_mutex_t mutex_dump_;
    std::string dump_path_;
    std::atomic<bool> dump_to_terminate_;
    std::map<std::string, pthread_t> dump_pthread_ts_;
    Snapshots dump_snapshots_;
};

}

#endif
