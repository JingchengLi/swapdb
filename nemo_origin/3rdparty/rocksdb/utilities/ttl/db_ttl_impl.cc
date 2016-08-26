// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/ttl/db_ttl_impl.h"

#include "rocksdb/utilities/convenience.h"
#include "rocksdb/utilities/db_ttl.h"
#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"

namespace rocksdb {

void DBWithTTLImpl::SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                                    Env* env) {
  if (options->compaction_filter) {
    options->compaction_filter =
        new TtlCompactionFilter(ttl, env, options->compaction_filter);
  } else {
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(new TtlCompactionFilterFactory(
            ttl, env, options->compaction_filter_factory));
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new TtlMergeOperator(options->merge_operator, env));
  }
}

// Open the db inside DBWithTTLImpl because options needs pointer to its ttl
DBWithTTLImpl::DBWithTTLImpl(DB* db) : DBWithTTL(db) {}

DBWithTTLImpl::DBWithTTLImpl(DB* db, int ttl) : DBWithTTL(db), ttl_(ttl) {
  }

DBWithTTLImpl::~DBWithTTLImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status UtilityDB::OpenTtlDB(const Options& options, const std::string& dbname,
                            StackableDB** dbptr, int32_t ttl, bool read_only) {
  DBWithTTL* db;
  Status s = DBWithTTL::Open(options, dbname, &db, ttl, read_only);
  if (s.ok()) {
    *dbptr = db;
  } else {
    *dbptr = nullptr;
  }
  return s;
}

Status DBWithTTL::Open(const Options& options, const std::string& dbname,
                       DBWithTTL** dbptr, int32_t ttl, bool read_only) {

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBWithTTL::Open(db_options, dbname, column_families, &handles,
                             dbptr, {ttl}, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBWithTTL::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBWithTTL** dbptr,
    std::vector<int32_t> ttls, bool read_only) {

  if (ttls.size() != column_families.size()) {
    return Status::InvalidArgument(
        "ttls size has to be the same as number of column families");
  }

  int key_ttl = 0;
  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBWithTTLImpl::SanitizeOptions(
        ttls[i], &column_families_sanitized[i].options,
        db_options.env == nullptr ? Env::Default() : db_options.env);
    key_ttl = ttls[i];
  }
  DB* db;

  Status st;
  if (read_only) {
    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
                             handles, &db);
  } else {
    st = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBWithTTLImpl(db, key_ttl);
    (*dbptr)->meta_prefix_ = db_options.meta_prefix;
  } else {
    *dbptr = nullptr;
  }
  return st;
}

Status DBWithTTLImpl::CreateColumnFamilyWithTtl(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle, int ttl) {
  ColumnFamilyOptions sanitized_options = options;
  DBWithTTLImpl::SanitizeOptions(ttl, &sanitized_options, GetEnv());

  return DBWithTTL::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

Status DBWithTTLImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  return CreateColumnFamilyWithTtl(options, column_family_name, handle, 0);
}

// Get the remained ttl accroding to the current timestamp and parameter
// -1 means never timeout, we treat get current time
int32_t DBWithTTLImpl::GetTTLFromNow(const Slice& value, int32_t ttl, Env* env) {

  int32_t timestamp_value =
      DecodeFixed32(value.data() + value.size() - kTSLength);
  if (timestamp_value == 0) {
      return -1;
  }

  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return -1;
  }

  int32_t ttl_from_now = ttl - (curtime - timestamp_value);
  return ttl_from_now >= 0 ? ttl_from_now : 0;
}

Status DBWithTTL::GetVersion(const Slice& key, int32_t *version) {
    // KV structure and data key of Hash, list, zset, set don't have version
    *version = 0;
    if (db_->GetMetaPrefix() == kMetaPrefix_KV || db_->GetMetaPrefix() != key[0]) {
      return Status::NotFound("Not meta key");
    }

    std::string value;
    Status st = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &value);
    if (!st.ok()) {
        return st;
    }

    if (value.size() < kVersionLength + kTSLength) {
      return Status::Corruption("Error: value's length less than version and timestamp's\n");
    }

    int32_t timestamp_value = DecodeFixed32(value.data() + value.size() - kTSLength);

    int64_t curtime;
    Env* env = db_->GetEnv();
    // Treat the data as fresh if could not get current time
    if (env->GetCurrentTime(&curtime).ok()) {
      if (timestamp_value != 0 && timestamp_value < curtime) { // 0 means fresh
        return Status::NotFound("Is stale\n");
      }
    }

    *version = DecodeFixed32(value.data() + value.size() - kTSLength - kVersionLength);

    return Status::OK();
}

// Get TTL, -1 means live forever
Status DBWithTTL::GetKeyTTL(const ReadOptions& options, const Slice& key, int32_t *ttl) {
    std::string value;
    Status st = db_->Get(options, db_->DefaultColumnFamily(), key, &value);
    if (!st.ok()) {
        return st;
    }

    if (value.size() < kVersionLength + kTSLength) {
      return Status::Corruption("Error: value's length less than version and timestamp's\n");
    }

    // KV do not need version check
    if (db_->GetMetaPrefix() == kMetaPrefix_KV) {
      if (DBWithTTLImpl::IsStale(value, 1, db_->GetEnv())) {
        *ttl = -2;
        return Status::NotFound("Is Stale");
      } else {
        *ttl = DBWithTTLImpl::GetTTLFromNow(value, 1, db_->GetEnv());
        return Status::OK();
      }
    } else {
      st = SanityCheckVersionAndTimestamp(key, value);
      if (st.ok()) {
        *ttl = DBWithTTLImpl::GetTTLFromNow(value, 1, db_->GetEnv());
        return Status::OK();
      } else {
        *ttl = -2;
        return st;
      }
    }
}

// Appends the current timestamp to the string.
// Returns false if could not get the current_time, true if append succeeds
Status DBWithTTLImpl::AppendTS(const Slice& val, std::string* val_with_ts,
                               Env* env) {
  val_with_ts->reserve(kTSLength + val.size());
  char ts_string[kTSLength];
  int64_t curtime;
  Status st = env->GetCurrentTime(&curtime);
  if (!st.ok()) {
    return st;
  }
  EncodeFixed32(ts_string, (int32_t)curtime);
  val_with_ts->append(val.data(), val.size());
  val_with_ts->append(ts_string, kTSLength);
  return st;
}

// Appends the caculated timestamp which equals to current timstamp add ttl.
// If ttl is non-positive, timestamp is 0 which means never timeout
//Status DBWithTTLImpl::AppendTSWithKeyTTL(const Slice& val, std::string* val_with_ts,
//                               Env* env, int32_t ttl) {
//  val_with_ts->reserve(kTSLength + val.size());
//  char ts_string[kTSLength];
//  if (ttl <= 0) {
//    EncodeFixed32(ts_string, 0);
//  } else {
//    int64_t curtime;
//    Status st = env->GetCurrentTime(&curtime);
//    if (!st.ok()) {
//        return st;
//    }
//    EncodeFixed32(ts_string, (int32_t)(curtime+ttl-1));
//  }
//  val_with_ts->append(val.data(), val.size());
//  val_with_ts->append(ts_string, kTSLength);
//  return Status::OK();
//}
//
//Status DBWithTTLImpl::AppendTSWithExpiredTime(const Slice& val, std::string* val_with_ts,
//                               Env* env, int32_t expired_time) {
//  val_with_ts->reserve(kTSLength + val.size());
//  char ts_string[kTSLength];
//  EncodeFixed32(ts_string, (int32_t)(expired_time-1));
//  val_with_ts->append(val.data(), val.size());
//  val_with_ts->append(ts_string, kTSLength);
//  return Status::OK();
//}

Status DBWithTTLImpl::AppendVersionAndExpiredTime(const Slice& val, std::string* val_with_ver_ts,
                               Env* env, int32_t version, int32_t expired_time) {
  val_with_ver_ts->reserve(kVersionLength + kTSLength + val.size());

  val_with_ver_ts->append(val.data(), val.size());

  char ver_string[kVersionLength];
  EncodeFixed32(ver_string, (int32_t)version);
  val_with_ver_ts->append(ver_string, kVersionLength);

  char ts_string[kTSLength];
  EncodeFixed32(ts_string, (int32_t)(expired_time-1));
  val_with_ver_ts->append(ts_string, kTSLength);

  return Status::OK();
}

Status DBWithTTLImpl::AppendVersionAndKeyTTL(const Slice& val, std::string* val_with_ver_ts,
                               Env* env, int32_t version, int32_t ttl) {
  val_with_ver_ts->reserve(kVersionLength + kTSLength + val.size());

  val_with_ver_ts->append(val.data(), val.size());

  char ver_string[kVersionLength];
  EncodeFixed32(ver_string, (int32_t)version);
  val_with_ver_ts->append(ver_string, kVersionLength);

  char ts_string[kTSLength];
  if (ttl <= 0) {
    EncodeFixed32(ts_string, 0);
  } else {
    int64_t curtime;
    Status st = env->GetCurrentTime(&curtime);
    if (!st.ok()) {
        return st;
    }
    EncodeFixed32(ts_string, (int32_t)(curtime+ttl-1));
  }
  val_with_ver_ts->append(ts_string, kTSLength);

  return Status::OK();
}

// Returns corruption if the length of the string is lesser than timestamp, or
// timestamp refers to a time lesser than ttl-feature release time
Status DBWithTTLImpl::SanityCheckTimestamp(const Slice& str) {
  if (str.size() < kVersionLength + kTSLength) {
    return Status::Corruption("Error: value's length less than version and timestamp's\n");
  }
  // Checks that TS is not lesser than kMinTimestamp
  // 0 means never timeout.
  // Gaurds against corruption & normal database opened incorrectly in ttl mode
  int32_t timestamp_value = DecodeFixed32(str.data() + str.size() - kTSLength);
  if (timestamp_value != 0 && timestamp_value < kMinTimestamp) {
    return Status::Corruption("Error: Timestamp < ttl feature release time!\n");
  }

  return Status::OK();
}

// Check version for hash, list, zset and set structures
// KV structure do not use version
Status DBWithTTL::SanityCheckVersionAndTimestamp(const Slice &key, const Slice &value) {
//  if (value.size() < kVersionLength + kTSLength) {
//    return Status::Corruption("Error: value's length less than version and timestamp's\n");
//  }

  std::string meta_value;

  int32_t timestamp_value = DecodeFixed32(value.data() + value.size() - kTSLength);
  // data key
  if (meta_prefix_ != key[0]) {
    std::string meta_key(1, meta_prefix_);
    int32_t len = *((uint8_t *)(key.data() + 1));
    meta_key.append(key.data() + 2, len);

    Status st = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), meta_key, &meta_value);
    if (st.ok()) {
      // Checks that Version is not older than key version
      int32_t meta_version = DecodeFixed32(meta_value.data() + meta_value.size() - kTSLength - kVersionLength);
      int32_t data_version = DecodeFixed32(value.data() + value.size() - kTSLength - kVersionLength);
      if (data_version < meta_version) {
        return Status::NotFound("old version\n");
      }
    }
    timestamp_value = DecodeFixed32(meta_value.data() + meta_value.size() - kTSLength);
  }

  int64_t curtime;
  Env* env = db_->GetEnv();
  // Treat the data as fresh if could not get current time
  if (env->GetCurrentTime(&curtime).ok()) {
    if (timestamp_value != 0 && timestamp_value < curtime) { // 0 means fresh
      return Status::NotFound("Is stale\n");
    }
  }

  return Status::OK();
}

// Checks if the string is stale or not according to TTl provided
bool DBWithTTLImpl::IsStale(const Slice& value, int32_t ttl, Env* env) {
 // if (ttl <= 0) {  // Data is fresh if TTL is non-positive
 //   return false;
 // }
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value = DecodeFixed32(value.data() + value.size() - kTSLength);
  if (timestamp_value == 0) { // 0 means fresh
      return false;
  }
  return timestamp_value < curtime - ttl;
}

bool DBWithTTLImpl::IsStale(int32_t timestamp, int32_t ttl, Env* env) {
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  if (timestamp == 0) { // 0 means fresh
      return false;
  }
  return timestamp < curtime - ttl;
}

// Strips the TS from the end of the string
Status DBWithTTLImpl::StripTS(std::string* str) {
  Status st;
  if (str->length() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return st;
}

// Strips the version and TS from the end of the string
Status DBWithTTLImpl::StripVersionAndTS(std::string* str) {
  Status st;
  if (str->length() < kTSLength + kVersionLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the version and TS
  str->erase(str->length() - kTSLength - kVersionLength, kTSLength + kVersionLength);
  return st;
}

// We treat Put as live forever,
// We will use 0 as a special TTL value and special appended TS
Status DBWithTTLImpl::Put(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val) {
  //int32_t version = db_->GetKeyVersion(key);
  WriteBatch batch;
  batch.Put(column_family, key, val);
  return WriteWithKeyTTL(options, &batch, ttl_);
}

// PutWithKeyTTL should use a positive TTL
Status DBWithTTL::PutWithKeyTTL(const WriteOptions& options, const Slice& key, const Slice& val, int32_t ttl) {
  //int32_t version = db_->GetKeyVersion(key);
  WriteBatch batch;
  batch.Put(db_->DefaultColumnFamily(), key, val);
  return DBWithTTL::WriteWithKeyTTL(options, &batch, ttl);
}

Status DBWithTTL::PutWithKeyVersion(const WriteOptions& options, const Slice& key, const Slice& val) {
  int32_t version = 0;
  GetVersion(key, &version);
  WriteBatch batch;
  batch.Put(db_->DefaultColumnFamily(), key, val);
  return DBWithTTL::WriteWithKeyVersion(options, &batch, version + 1);
}

Status DBWithTTL::PutWithExpiredTime(const WriteOptions& options, const Slice& key, const Slice& val, int32_t expired_time) {
  //int32_t version = db_->GetKeyVersion(key);
  WriteBatch batch;
  batch.Put(db_->DefaultColumnFamily(), key, val);
  return DBWithTTL::WriteWithExpiredTime(options, &batch, expired_time);
}

Status DBWithTTLImpl::Get(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          std::string* value) {
  Status st = db_->Get(options, column_family, key, value);
  if (!st.ok()) {
    return st;
  }

  if (value->size() < kVersionLength + kTSLength) {
    return Status::Corruption("Error: value's length less than version and timestamp's\n");
  }

  // KV do not need version check
  if (db_->GetMetaPrefix() == kMetaPrefix_KV) {
    if (DBWithTTLImpl::IsStale(*value, 1, db_->GetEnv())) {
      *value = "";
      return Status::NotFound("Is Stale");
    } else {
      return StripVersionAndTS(value);
    }
  } else {
    st = SanityCheckVersionAndTimestamp(key, *value);
    if (st.ok()) {
      return StripVersionAndTS(value);
    } else {
      *value = "";
      return st;
    }
  }
}

// not used
// @TODO fix this
std::vector<Status> DBWithTTLImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto statuses = db_->MultiGet(options, column_family, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = SanityCheckTimestamp((*values)[i]);
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = StripTS(&(*values)[i]);
  }
  return statuses;
}

bool DBWithTTLImpl::KeyMayExist(const ReadOptions& options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value,
                                bool* value_found) {
  bool ret = db_->KeyMayExist(options, column_family, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBWithTTLImpl::Merge(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) {
  //int32_t version = db_->GetKeyVersion(key);
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  //return Write(options, &batch);
  return WriteWithKeyTTL(options, &batch, ttl_);
}

Status DBWithTTLImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler(Env* env) : env_(env) {}
    WriteBatch updates_ttl;
    Status batch_rewrite_status;
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendTS(value, &value_with_ts, env_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendTS(value, &value_with_ts, env_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
  };
  Handler handler(GetEnv());
  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTL::WriteWithKeyTTL(const WriteOptions& opts, WriteBatch* updates, int32_t ttl) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, int32_t ttl, DB* db)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env), ttl_(ttl) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t tmp_timestamp;
      db_->GetKeyVersionAndTS(key, &version, &tmp_timestamp);

      Status st = DBWithTTLImpl::AppendVersionAndKeyTTL(value, &value_with_ver_ts, env_, version, ttl_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t tmp_timestamp;
      db_->GetKeyVersionAndTS(key, &version, &tmp_timestamp);

      Status st = DBWithTTLImpl::AppendVersionAndKeyTTL(value, &value_with_ver_ts, env_, version, ttl_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) { updates_ttl.PutLogData(blob); }

   private:
    Env* env_;
    int32_t ttl_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), ttl, db_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTL::WriteWithOldKeyTTL(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, DB* db)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t timestamp;
      db_->GetKeyVersionAndTS(key, &version, &timestamp);

      int64_t curtime;
      if (env_->GetCurrentTime(&curtime).ok()) {
          if (timestamp != 0 && timestamp < curtime) {
              version++;
              timestamp = 0;
          }
      } else {
          timestamp = 0;
      }

      Status st = DBWithTTLImpl::AppendVersionAndExpiredTime(value, &value_with_ver_ts, env_, version, timestamp + 1);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t timestamp;
      db_->GetKeyVersionAndTS(key, &version, &timestamp);

      int64_t curtime;
      if (env_->GetCurrentTime(&curtime).ok()) {
          if (timestamp != 0 && timestamp < curtime) {
              version++;
              timestamp = 0;
          }
      } else {
          timestamp = 0;
      }

      Status st = DBWithTTLImpl::AppendVersionAndExpiredTime(value, &value_with_ver_ts, env_, version, timestamp + 1);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) { updates_ttl.PutLogData(blob); }

   private:
    Env* env_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), db_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTL::WriteWithExpiredTime(const WriteOptions& opts, WriteBatch* updates, int32_t expired_time) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, int32_t expired_time, DB* db)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env), expired_time_(expired_time) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t timestamp;
      db_->GetKeyVersionAndTS(key, &version, &timestamp);

      Status st = DBWithTTLImpl::AppendVersionAndExpiredTime(value, &value_with_ver_ts, env_, version, expired_time_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      std::string value_with_ver_ts;
      int32_t version;
      int32_t timestamp;
      db_->GetKeyVersionAndTS(key, &version, &timestamp);

      Status st = DBWithTTLImpl::AppendVersionAndExpiredTime(value, &value_with_ver_ts, env_, version, expired_time_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) { updates_ttl.PutLogData(blob); }

   private:
    Env* env_;
    int32_t expired_time_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), expired_time, db_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTL::WriteWithKeyVersion(const WriteOptions& opts, WriteBatch* updates, int32_t version) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, int32_t version, DB* db)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env), version_(version) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      std::string value_with_ver_ts;
      Status st = DBWithTTLImpl::AppendVersionAndKeyTTL(value, &value_with_ver_ts, env_, version_, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      std::string value_with_ver_ts;
      Status st = DBWithTTLImpl::AppendVersionAndKeyTTL(value, &value_with_ver_ts, env_, version_, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) { updates_ttl.PutLogData(blob); }

   private:
    Env* env_;
    int32_t version_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), version, db_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Iterator* DBWithTTLImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new TtlIterator(db_->NewIterator(opts, column_family), db_);
}

bool TtlCompactionFilter::Filter(int level, const Slice& key, const Slice& old_val,
                                 std::string* new_val, bool* value_changed) const {
  if (meta_prefix_ == kMetaPrefix_KV) {
    if (DBWithTTLImpl::IsStale(old_val, 0, env_)) {
      return true;
    }
  } else {
    // reserve meta key for hash, list, zset, set
    if (key[0] == meta_prefix_) {
      return false;
    }

    // reserve the separator of meta and data for multi-structures
    if (key.size() == 1) {
      return false;
    }

    if (DBWithTTLImpl::IsStale(meta_timestamp_, 0, env_)) {
      return true;
    }

    int32_t member_version = DecodeFixed32(old_val.data() + old_val.size() - DBImpl::kVersionLength - DBImpl::kTSLength);
    if (member_version < meta_version_) {
      return true;
    }
  }

  if (user_comp_filter_ == nullptr) {
    return false;
  }
  assert(old_val.size() >= DBImpl::kTSLength);
  Slice old_val_without_ts(old_val.data(),
                           old_val.size() - DBImpl::kTSLength);
  if (user_comp_filter_->Filter(level, key, old_val_without_ts, new_val,
                                value_changed)) {
    return true;
  }
  if (*value_changed) {
    new_val->append(
        old_val.data() + old_val.size() - DBImpl::kTSLength,
        DBImpl::kTSLength);
  }
  return false;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
