#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <string>
#include <errno.h>

#include "nemo.h"
#include "nemo_mutex.h"
#include "nemo_const.h"
#include "nemo_iterator.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "nemo_set.h"
#include "nemo_list.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

const std::string DEFAULT_BG_PATH = "dump";

Status Nemo::SaveDBWithTTL(const std::string &db_path, const std::string &key_type, const char meta_prefix, std::unique_ptr<rocksdb::DBWithTTL> &src_db, const rocksdb::Snapshot *snapshot) {
  //printf ("db_path=%s\n", db_path.c_str());

  rocksdb::DBWithTTL *dst_db;
  rocksdb::Options option(open_options_);
  option.meta_prefix = meta_prefix;

  rocksdb::Status s = rocksdb::DBWithTTL::Open(option, db_path, &dst_db);
  if (!s.ok()) {
    log_err("save db %s, open error %s", db_path.c_str(), s.ToString().c_str());
    return s;
  }

  //printf ("\nSaveDBWithTTL seqnumber=%d\n", snapshot->GetSequenceNumber());
  int64_t ttl;
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = snapshot;
  iterate_options.fill_cache = false;

  rocksdb::Iterator* it = src_db->NewIterator(iterate_options);
  for (it->SeekToFirst(); it->Valid() && !dump_to_terminate_; it->Next()) {
    std::string raw_key(it->key().data(), it->key().size());

    if (key_type == KV_DB) {
      s = KTTL(it->key().ToString(), &ttl);
    } else if (key_type == HASH_DB) {
      if (raw_key[0] == meta_prefix) {
        s = HTTL(raw_key.substr(1), &ttl);
        //printf ("0.1 DB:%s meta_prefix=%c raw_key(%s) ttl return %s, ttl is %ld\n", key_type.c_str(), meta_prefix, raw_key.c_str(), s.ToString().c_str(), ttl);
      } else {
        int32_t len = *((uint8_t *)(raw_key.data() + 1));
        std::string key(raw_key.data() + 2, len);
        s = HTTL(key, &ttl);
        //printf ("0.2 DB:%s meta_prefix=%c raw_key(%s) key(%s) ttl return %s, ttl is %ld\n", key_type.c_str(), meta_prefix, raw_key.c_str(), key.c_str(), s.ToString().c_str(), ttl);
      }
    } else if (key_type == LIST_DB) {
      if (raw_key[0] == meta_prefix) {
        s = LTTL(raw_key.substr(1), &ttl);
      } else {
        int32_t len = *((uint8_t *)(raw_key.data() + 1));
        std::string key(raw_key.data() + 2, len);
        s = LTTL(key, &ttl);
      }
    } else if (key_type == ZSET_DB) {
      if (raw_key[0] == meta_prefix) {
        s = ZTTL(raw_key.substr(1), &ttl);
      } else {
        int32_t len = *((uint8_t *)(raw_key.data() + 1));
        std::string key(raw_key.data() + 2, len);
        s = ZTTL(key, &ttl);
      }
    } else if (key_type == SET_DB) {
      if (raw_key[0] == meta_prefix) {
        s = STTL(raw_key.substr(1), &ttl);
      } else {
        int32_t len = *((uint8_t *)(raw_key.data() + 1));
        std::string key(raw_key.data() + 2, len);
        s = STTL(key, &ttl);
      }
    }
    if (s.ok()) {
      if (ttl == -1) {
        s = dst_db->Put(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString());
      } else if (ttl > 0) {
        s = dst_db->PutWithKeyTTL(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString(), ttl);
      }
    }
  }
  delete it;
  //src_db->ReleaseSnapshot(iterate_options.snapshot);
  delete dst_db;

  return Status::OK();
}

Status Nemo::SaveDB(const std::string &db_path, std::unique_ptr<rocksdb::DB> &src_db, const rocksdb::Snapshot *snapshot) {
  if (opendir(db_path.c_str()) == NULL) {
    mkdir(db_path.c_str(), 0755);
  }

  //printf ("db_path=%s\n", db_path.c_str());

  rocksdb::DB* dst_db;
  rocksdb::Status s = rocksdb::DB::Open(open_options_, db_path, &dst_db);
  if (!s.ok()) {
    log_err("save db %s, open error %s", db_path.c_str(), s.ToString().c_str());
    return s;
  }

  //printf ("\nSaveDB seqnumber=%d\n", snapshot->GetSequenceNumber());
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = snapshot;
  iterate_options.fill_cache = false;

  rocksdb::Iterator* it = src_db->NewIterator(iterate_options);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // printf ("SaveDB key=(%s) value=(%s) val_size=%u\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
    //        it->value().ToString().size());

    s = dst_db->Put(rocksdb::WriteOptions(), it->key().ToString(), it->value().ToString());
  }
  delete it;
  src_db->ReleaseSnapshot(iterate_options.snapshot);
  delete dst_db;

  return Status::OK();
}

//Status Nemo::BGSaveReleaseSnapshot(Snapshots &snapshots) {
//
//    // Note the order which is decided by GetSnapshot
//    kv_db_->ReleaseSnapshot(snapshots[0]);
//    hash_db_->ReleaseSnapshot(snapshots[1]);
//    zset_db_->ReleaseSnapshot(snapshots[2]);
//    set_db_->ReleaseSnapshot(snapshots[3]);
//    list_db_->ReleaseSnapshot(snapshots[4]);
//
//    return Status::OK();
//}

Status Nemo::BGSaveGetSpecifySnapshot(const std::string key_type, Snapshot * &snapshot) {

  if (key_type == KV_DB) {
    snapshot = kv_db_->GetSnapshot();
    if (snapshot == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
  } else if (key_type == HASH_DB) {
    snapshot = hash_db_->GetSnapshot();
    if (snapshot == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
  } else if (key_type == LIST_DB) {
    snapshot = list_db_->GetSnapshot();
    if (snapshot == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
  } else if (key_type == ZSET_DB) {
    snapshot = zset_db_->GetSnapshot();
    if (snapshot == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
  } else if (key_type == SET_DB) {
    snapshot = set_db_->GetSnapshot();
    if (snapshot == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
  } else {
    return Status::InvalidArgument("");
  }
  return Status::OK();
}

Status Nemo::BGSaveGetSnapshot(Snapshots &snapshots) {
  const rocksdb::Snapshot* psnap;

  psnap = kv_db_->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  snapshots.push_back(psnap);

  psnap = hash_db_->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  snapshots.push_back(psnap);

  psnap = zset_db_->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  snapshots.push_back(psnap);

  psnap = set_db_->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  snapshots.push_back(psnap);

  psnap = list_db_->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  snapshots.push_back(psnap);

  return Status::OK();
}

struct SaveArgs {
  void *p;
  const std::string key_type;
  Snapshot *snapshot;

  SaveArgs(void *_p, const std::string &_key_type, Snapshot* _snapshot)
      : p(_p), key_type(_key_type), snapshot(_snapshot) {};
};

void* call_BGSaveSpecify(void *arg) {

  Nemo* p = (Nemo*)(((SaveArgs*)arg)->p);
  Snapshot* snapshot = ((SaveArgs*)arg)->snapshot;
  std::string key_type = ((SaveArgs*)arg)->key_type;
  Status s = p->BGSaveSpecify(key_type, snapshot);
  return nullptr;
}

Status Nemo::BGSaveSpecify(const std::string key_type, Snapshot* snapshot) {
  //void* Nemo::BGSaveSpecify(void *arg) {
  Status s;

  if (key_type == KV_DB) {
    s = SaveDBWithTTL(dump_path_ + KV_DB, key_type, '\0', kv_db_, snapshot);
  } else if (key_type == HASH_DB) {
    s = SaveDBWithTTL(dump_path_ + HASH_DB, key_type, DataType::kHSize, hash_db_, snapshot);
  } else if (key_type == ZSET_DB) {
    s = SaveDBWithTTL(dump_path_ + ZSET_DB, key_type, DataType::kZSize, zset_db_, snapshot);
  } else if (key_type == SET_DB) {
    s = SaveDBWithTTL(dump_path_ + SET_DB, key_type, DataType::kSSize, set_db_, snapshot);
  } else if (key_type == LIST_DB) {
    s = SaveDBWithTTL(dump_path_ + LIST_DB, key_type, DataType::kLMeta, list_db_, snapshot);
  } else {
    s = Status::InvalidArgument("");
  }

  {
    MutexLock l(&mutex_dump_);
    dump_pthread_ts_.erase(key_type);
  }
  return s;
}


Status Nemo::BGSave(Snapshots &snapshots, const std::string &db_path) {
  std::string path = db_path;
  if (path.empty()) {
    path = DEFAULT_BG_PATH;
  }

  if (path[path.length() - 1] != '/') {
    path.append("/");
  }
  if (opendir(path.c_str()) == NULL) {
    mkpath(path.c_str(), 0755);
  }

  {
    MutexLock l(&mutex_dump_);
    if (dump_pthread_ts_.size() != 0 || dump_snapshots_.size() != 0) {
      return Status::Corruption("DB dumping is performing.");
    }
    dump_path_ = path;
    dump_to_terminate_ = false;
  }

  Status s;

  pthread_t tid[5];
  SaveArgs *arg_kv = new SaveArgs(this, KV_DB, snapshots[0]);
  if (pthread_create(&tid[0], NULL, &call_BGSaveSpecify, arg_kv) != 0) {
    return Status::Corruption("pthead_create failed.");
  }

  SaveArgs *arg_hash = new SaveArgs(this, HASH_DB, snapshots[1]);
  if (pthread_create(&tid[1], NULL, &call_BGSaveSpecify, arg_hash) != 0) {
    return Status::Corruption("pthead_create failed.");
  }

  SaveArgs *arg_zset = new SaveArgs(this, ZSET_DB, snapshots[2]);
  if (pthread_create(&tid[2], NULL, &call_BGSaveSpecify, arg_zset) != 0) {
    return Status::Corruption("pthead_create failed.");
  }

  SaveArgs *arg_set = new SaveArgs(this, SET_DB, snapshots[3]);
  if (pthread_create(&tid[3], NULL, &call_BGSaveSpecify, arg_set) != 0) {
    return Status::Corruption("pthead_create failed.");
  }

  SaveArgs *arg_list = new SaveArgs(this, LIST_DB, snapshots[4]);
  if (pthread_create(&tid[4], NULL, &call_BGSaveSpecify, arg_list) != 0) {
    return Status::Corruption("pthead_create failed.");
  }

  {
    MutexLock l(&mutex_dump_);
    dump_pthread_ts_[KV_DB] = tid[0];
    dump_pthread_ts_[HASH_DB] = tid[1];
    dump_pthread_ts_[ZSET_DB] = tid[2];
    dump_pthread_ts_[SET_DB] = tid[3];
    dump_pthread_ts_[LIST_DB] = tid[4];
    dump_snapshots_ = snapshots;
  }

  int ret;
  void *retval;
  for (int i = 0; i < 5; i++) {
    if ((ret = pthread_join(tid[i], &retval)) != 0) {
      std::string msg = std::to_string(ret);
      return Status::Corruption("pthead_join failed with " + msg);
    }
  }

  {
    MutexLock l(&mutex_dump_);
    kv_db_->ReleaseSnapshot(dump_snapshots_[0]);
    hash_db_->ReleaseSnapshot(dump_snapshots_[1]);
    zset_db_->ReleaseSnapshot(dump_snapshots_[2]);
    set_db_->ReleaseSnapshot(dump_snapshots_[3]);
    list_db_->ReleaseSnapshot(dump_snapshots_[4]);
    dump_pthread_ts_.clear();
    dump_snapshots_.clear();
    dump_to_terminate_ = true;
  }

  delete arg_kv;
  delete arg_hash;
  delete arg_zset;
  delete arg_set;
  delete arg_list;

  return Status::OK();
}

Status Nemo::BGSaveOff() {
  sleep(1);
  dump_to_terminate_ = true;
  return Status::OK();
}

#if 0
Status Nemo::BGSave(Snapshots &snapshots, const std::string &db_path) {
  if (save_flag_) {
    return Status::Corruption("Already saving");
  }

  // maybe need lock
  save_flag_ = true;

  std::string path = db_path;
  if (path.empty()) {
    path = DEFAULT_BG_PATH;
  }

  if (path[path.length() - 1] != '/') {
    path.append("/");
  }
  if (opendir(path.c_str()) == NULL) {
    mkpath(path.c_str(), 0755);
  }

  Status s;
  s = SaveDBWithTTL(path + KV_DB, kv_db_, snapshots[0]);
  if (!s.ok()) return s;

  s = SaveDB(path + HASH_DB, hash_db_, snapshots[1]);
  if (!s.ok()) return s;

  s = SaveDB(path + ZSET_DB, zset_db_, snapshots[2]);
  if (!s.ok()) return s;

  s = SaveDB(path + SET_DB, set_db_, snapshots[3]);
  if (!s.ok()) return s;

  s = SaveDB(path + LIST_DB, list_db_, snapshots[4]);
  if (!s.ok()) return s;

  save_flag_ = false;

  //BGSaveReleaseSnapshot(snapshots);

  return Status::OK();
}
#endif

Status Nemo::ScanKeyNumWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, uint64_t &num) {
  rocksdb::ReadOptions iterate_options;

  iterate_options.snapshot = db->GetSnapshot();
  iterate_options.fill_cache = false;

  rocksdb::Iterator *it = db->NewIterator(iterate_options);

  num = 0;
  it->SeekToFirst();
  for (; it->Valid() && !scan_keynum_exit_; it->Next()) {
    num++;
    //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
    //       it->value().ToString().size(), num);
  }

//  if (scan_keynum_exit_ == true) {
//    printf ("Stop flag so we exit TTL\n");
//  }

  db->ReleaseSnapshot(iterate_options.snapshot);
  delete it;

  return Status::OK();
}

Status Nemo::ScanKeyNum(std::unique_ptr<rocksdb::DBWithTTL> &db, const char kType, uint64_t &num) {
  rocksdb::ReadOptions iterate_options;

  iterate_options.snapshot = db->GetSnapshot();
  iterate_options.fill_cache = false;

  rocksdb::Iterator *it = db->NewIterator(iterate_options);
  std::string key_start = "a";
  key_start[0] = kType;
  it->Seek(key_start);

  num = 0;
  for (; it->Valid() && !scan_keynum_exit_; it->Next()) {
    if (kType != it->key().ToString().at(0)) {
      break;
    }
    num++;
    //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
    //       it->value().ToString().size(), num);
  }

//  if (scan_keynum_exit_ == true) {
//    printf ("Stop flag so we exit TTL\n");
//  }

  db->ReleaseSnapshot(iterate_options.snapshot);
  delete it;

  return Status::OK();
}

Status Nemo::GetSpecifyKeyNum(const std::string type, uint64_t &num) {
  if (type == KV_DB) {
    ScanKeyNumWithTTL(kv_db_, num);
  } else if (type == HASH_DB) {
    ScanKeyNum(hash_db_, DataType::kHSize, num);
  } else if (type == LIST_DB) {
    ScanKeyNum(list_db_,  DataType::kLMeta, num);
  } else if (type == ZSET_DB) {
    ScanKeyNum(zset_db_, DataType::kZSize, num);
  } else if (type == SET_DB) {
    ScanKeyNum(set_db_, DataType::kSSize, num);
  } else {
    return Status::InvalidArgument("");
  }

  return Status::OK();
}

Status Nemo::GetKeyNum(std::vector<uint64_t>& nums) {
  uint64_t num;

  if (!scan_keynum_exit_) {
    ScanKeyNumWithTTL(kv_db_, num);
    nums.push_back(num);
  }

  if (!scan_keynum_exit_) {
    ScanKeyNum(hash_db_, DataType::kHSize, num);
    nums.push_back(num);
  }

  if (!scan_keynum_exit_) {
    ScanKeyNum(list_db_,  DataType::kLMeta, num);
    nums.push_back(num);
  }

  if (!scan_keynum_exit_) {
    ScanKeyNum(zset_db_, DataType::kZSize, num);
    nums.push_back(num);
  }

  if (!scan_keynum_exit_) {
    ScanKeyNum(set_db_, DataType::kSSize, num);
    nums.push_back(num);
  }

  if (scan_keynum_exit_) {
    scan_keynum_exit_ = false;
    return Status::Corruption("exit");
  }
  return Status::OK();
}

Status Nemo::StopScanKeyNum() {
  sleep(1);
  scan_keynum_exit_ = true;
  return Status::OK();
}

Status Nemo::DoCompact(DBType type) {
  if (type != kALL && type != kKV_DB && type != kHASH_DB &&
      type != kZSET_DB && type != kSET_DB && type != kLIST_DB) {
      return Status::InvalidArgument("");
  }

  current_task_type_ = OPERATION::kCLEAN_ALL;

  Status s;
  if (type == kALL || type == kKV_DB) {
    s = kv_db_->CompactRange(NULL, NULL);
  }
  if (type == kALL || type == kHASH_DB) {
    s = hash_db_->CompactRange(NULL, NULL);
  }
  if (type == kALL || type == kZSET_DB) {
    s = zset_db_->CompactRange(NULL, NULL);
  }
  if (type == kALL || type == kSET_DB) {
    s = set_db_->CompactRange(NULL, NULL);
  }
  if (type == kALL || type == kLIST_DB) {
    s = list_db_->CompactRange(NULL, NULL);
  }

  current_task_type_ = OPERATION::kNONE_OP;
  return s;
}

Status Nemo::Compact(DBType type, bool sync){
  Status s;

  if (sync) {
    s = DoCompact(type);
  } else {
    std::string tmp;
    AddBGTask({type, OPERATION::kCLEAN_ALL, tmp, tmp});
  }
  return s;
}

std::string Nemo::GetCurrentTaskType() {
  int type = current_task_type_;
  switch (type) {
    case kDEL_KEY:
      return "Key";
    case kCLEAN_ALL:
      return "All";
    case kNONE_OP:
    default:
      return "No";
  }
}

// should be replaced by the one with DBType parameter
rocksdb::DBWithTTL* Nemo::GetDBByType(const std::string& type) {
  if (type == KV_DB)
    return kv_db_.get();
  else if (type == HASH_DB)
    return hash_db_.get();
  else if (type == LIST_DB)
    return list_db_.get();
  else if (type == SET_DB)
    return set_db_.get();
  else if (type == ZSET_DB)
    return zset_db_.get();
  else
    return NULL;
}

void FindLongSuccessor(std::string* key) {
  size_t n = key->size();
  for (size_t i = n - 1; i > 0; i--) {
    const uint8_t byte = (*key)[i];
    if (byte != static_cast<uint8_t>(0xff)) {
      (*key)[i] = byte + 1;
      key->resize(i+1);
      return;
    }
  }
}

//
// BGTask related 
Status Nemo::CompactKey(const DBType type, const rocksdb::Slice& key) {
  std::string key_begin;
  std::string key_end;

  current_task_type_ = OPERATION::kDEL_KEY;

  if (type == DBType::kALL || type == DBType::kHASH_DB) {
    key_begin = EncodeHashKey(key, "");
    key_end = key_begin;
    FindLongSuccessor(&key_end);
    rocksdb::Slice sb(key_begin);
    rocksdb::Slice se(key_end);

    hash_db_->CompactRange(&sb, &se);
  }

  if (type == DBType::kALL || type == DBType::kLIST_DB) {
    key_begin = EncodeListKey(key, 0);
    key_end = EncodeListKey(key, -1);
    //key_end = key_begin;
    //key_end.append(str);
    //FindLongSuccessor(key_end);
    rocksdb::Slice sb(key_begin);
    rocksdb::Slice se(key_end);
    //int result = memcmp(sb.data(), se.data(), sb.size());
    //printf ("LIST Memcmp sb=(%s),%u se=(%s),%u return %d\n", sb.data(), sb.size(), se.data(), se.size(), result);
    //printf (" Slice.compare return %d\n", sb.compare(se));

    list_db_->CompactRange(&sb, &se);
  }

  if (type == DBType::kALL || type == DBType::kSET_DB) {
    key_begin = EncodeSetKey(key, "");
    key_end = key_begin;
    FindLongSuccessor(&key_end);
    rocksdb::Slice sb(key_begin);
    rocksdb::Slice se(key_end);

    set_db_->CompactRange(&sb, &se);
  }

  if (type == DBType::kALL || type == DBType::kZSET_DB) {
    key_begin = EncodeZSetKey(key, "");
    key_end = key_begin;
    FindLongSuccessor(&key_end);
    rocksdb::Slice sb(key_begin);
    rocksdb::Slice se(key_end);
    //int result = memcmp(sb.data(), se.data(), sb.size());
    //printf ("ZSET Memcmp sb=(%s),%u se=(%s),%u return %d\n", sb.data(), sb.size(), se.data(), se.size(), result);
    //printf (" Slice.compare return %d\n", sb.compare(se));
    zset_db_->CompactRange(&sb, &se);

    key_begin = EncodeZScoreKey(key, "", ZSET_SCORE_MIN);
    key_end = EncodeZScoreKey(key, "", ZSET_SCORE_MAX);
    rocksdb::Slice zb(key_begin);
    rocksdb::Slice ze(key_end);

    zset_db_->CompactRange(&zb, &ze);
  }

  current_task_type_ = OPERATION::kNONE_OP;

  return Status::OK();
}

Status Nemo::AddBGTask(const BGTask& task) {
  //printf ("AddBGTask task{ type=%d, op=%d argv1= %s}\n", task.type, task.op, task.argv1.c_str());
  mutex_bgtask_.Lock();
  bg_tasks_.push(task);
  //printf ("AddBGTask push task{ type=%d, op=%d argv1= %s}, Signal\n", task.type, task.op, task.argv1.c_str());
  bg_cv_.Signal();
  mutex_bgtask_.Unlock();
  //printf ("AddBGTask push task{ type=%d, op=%d argv1= %s}, after Signal\n", task.type, task.op, task.argv1.c_str());

  return Status::OK();
}

Status Nemo::RunBGTask() {
  BGTask task;

  while (bgtask_flag_) {
    //printf ("RunBGTask main loop\n");
    mutex_bgtask_.Lock();
    //printf ("RunBGTask main loop mutex-lock\n");

    while (bg_tasks_.empty() && bgtask_flag_ ) {
      //printf ("RunBGTask main loop task.empty, bg_cv_ will Wait\n");
      bg_cv_.Wait();
      //printf ("RunBGTask main loop task.empty, bg_cv_ startup\n");
    }

    if (!bg_tasks_.empty()) {
      task = bg_tasks_.front();
      bg_tasks_.pop();
    }
    mutex_bgtask_.Unlock();

    if (!bgtask_flag_) {
      return Status::Incomplete("bgtask return with bgtask_flag_ false");
    }

    switch (task.op) {
      case kDEL_KEY:
        //printf ("BGTask run task { type=%d, argv1=%s}\n", task.type, task.argv1.c_str());
        CompactKey(task.type, task.argv1);
        break;
      case kCLEAN_RANGE:
        break;
      case kCLEAN_ALL:
        DoCompact(task.type);
        break;
      default:
        break;
    }
  }

  return Status::OK();
}

static void* StartBGThreadWrapper(void* arg) {
  Nemo* n = reinterpret_cast<Nemo*>(arg);
  n->RunBGTask();
  return NULL;
}

Status Nemo::StartBGThread() {
  int result = pthread_create(&bg_tid_, NULL,  &StartBGThreadWrapper, this);
  if (result != 0) {
    char msg[128];
    snprintf (msg, 128, "pthread create: %s", strerror(result));
    return Status::Corruption(msg);
  }

  return Status::OK();
}

uint64_t Nemo::GetProperty(const std::string &property) {
  uint64_t result = 0;
  char *pEnd;
  std::string out;

  kv_db_->GetProperty(property, &out);
  result += std::strtoull(out.c_str(), &pEnd, 10);
  hash_db_->GetProperty(property, &out);
  result += std::strtoull(out.c_str(), &pEnd, 10);
  zset_db_->GetProperty(property, &out);
  result += std::strtoull(out.c_str(), &pEnd, 10);
  set_db_->GetProperty(property, &out);
  result += std::strtoull(out.c_str(), &pEnd, 10);
  list_db_->GetProperty(property, &out);
  result += std::strtoull(out.c_str(), &pEnd, 10);

  //printf ("cur-size-all-mem-tables: (%s)\n", out.c_str());
  return result;
}

uint64_t Nemo::GetLockUsage() {
  int64_t result = 0;
  result += mutex_hash_record_.GetUsage();
  result += mutex_zset_record_.GetUsage();
  result += mutex_set_record_.GetUsage();
  result += mutex_list_record_.GetUsage();

  return result;
}

Status Nemo::GetUsage(const std::string& type, uint64_t *result) {
  *result = 0;

  // TODO rm
  //std::shared_ptr<TableFactory> table_factory;
  //const BlockBasedTableOptions& GetTableOptions() const;
  
//  std::shared_ptr<rocksdb::TableFactory> kv_tf = kv_db_->GetOptions().table_factory;
//  //kv_tf->GetTableOptions();
//  int64_t block_cache = (dynamic_cast<rocksdb::BlockBasedTableFactory *>(kv_tf))->GetTableOptions().block_cache->GetUsage();
//  //int64_t pin_usage = kv_tf->GetTableOptions().block_cache->GetPinnedUsage();
//  printf ("block_cache : %ld\n", block_cache);
//  //printf ("pin_usage : %ld\n", pin_usage);


//  rocksdb::BlockBasedTableOptions table_options;
//  *result = table_options.block_cache->GetUsage();
//  printf ("block_cache usage:%lu\n", *result);
//  *result = table_options.block_cache->GetPinnedUsage();
//  printf ("block_cache pinned usage:%lu\n", *result);

  //kv_db_->GetProperty("rocksdb.estimate-table-readers-mem", &out);
  //printf ("table_readers: (%s)\n", out.c_str());

  // Rocksdb part
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_ROCKSDB || type == USAGE_TYPE_ROCKSDB_MEMTABLE) {
    *result += GetProperty("rocksdb.cur-size-all-mem-tables");
  }
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_ROCKSDB || type == USAGE_TYPE_ROCKSDB_TABLE_READER) {
    *result += GetProperty("rocksdb.estimate-table-readers-mem");
  }
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_NEMO) {
    *result += GetLockUsage(); 
  }

  return Status::OK();
}
