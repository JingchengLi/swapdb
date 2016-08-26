#include "nemo_hash.h"
#include "nemo_list.h"
#include "nemo_set.h"
#include "nemo_zset.h"

namespace nemo {

bool NemoMeta::Create(DBType type, MetaPtr &p_meta){
  switch (type) {
  case kHASH_DB:
    p_meta.reset(new HashMeta());
    break;
  case kLIST_DB:
    p_meta.reset(new ListMeta());
    break;
  case kSET_DB:
    p_meta.reset(new SetMeta());
    break;
  case kZSET_DB:
    p_meta.reset(new ZSetMeta());
  default:
    return false;
  }
  return true;
}

char Nemo::GetMetaPrefix(DBType type) {
  switch (type) {
  case kHASH_DB:
    return DataType::kHSize;
  case kLIST_DB:
    return DataType::kLMeta;
  case kSET_DB:
    return DataType::kSSize;
  case kZSET_DB:
    return DataType::kZSize;
  default:
    return '\0';
  }
}

Status Nemo::ScanMetasSpecify(DBType type, const std::string &pattern,
    std::map<std::string, MetaPtr>& metas) {
  switch (type) {
  case kHASH_DB:
    return ScanDBMetas(hash_db_, type, pattern, metas);
  case kLIST_DB:
    return ScanDBMetas(list_db_, type, pattern, metas);
  case kSET_DB:
    return ScanDBMetas(set_db_, type, pattern, metas);
  case kZSET_DB:
    return ScanDBMetas(zset_db_, type, pattern, metas);
  default:
    return Status::InvalidArgument("error db type");
  }
}

Status Nemo::ScanDBMetas(std::unique_ptr<rocksdb::DBWithTTL> &db,
    DBType type, const std::string &pattern, std::map<std::string, MetaPtr>& metas) {
  // Get Current Snapshot
  const rocksdb::Snapshot* psnap;
  psnap = db->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  Status s = ScanDBMetasOnSnap(db, psnap, type, pattern, metas);
  db->ReleaseSnapshot(psnap);
  return s;
}

Status Nemo::ScanDBMetasOnSnap(std::unique_ptr<rocksdb::DBWithTTL> &db, const rocksdb::Snapshot* psnap,
    DBType type, const std::string &pattern, std::map<std::string, MetaPtr>& metas) {
  // Get meta prefix and db handler
  std::string prefix = std::string(1, GetMetaPrefix(type));
  if (prefix.empty()) {
    return Status::InvalidArgument("Error db type");
  }

  // Create Iterator
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = psnap;
  iterate_options.fill_cache = false;
  rocksdb::Iterator *it = db->NewIterator(iterate_options);

  it->Seek(prefix);
  MetaPtr p_meta;
  NemoMeta::Create(type, p_meta);
  for (; it->Valid(); it->Next()) {
    if (prefix.at(0) != it->key().ToString().at(0)) {
      break;
    }
    std::string key = it->key().ToString().substr(1);
    if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
      p_meta->DecodeFrom(it->value().ToString());
      metas.insert(std::make_pair(key, p_meta));
    }
  }

  //db->ReleaseSnapshot(psnap);
  delete it;
  return Status::OK();
}

Status Nemo::CheckMetaSpecify(DBType type, const std::string &pattern) {
  switch (type) {
  case kHASH_DB:
    return CheckDBMeta(hash_db_, type, pattern);
  case kLIST_DB:
    return CheckDBMeta(list_db_, type, pattern);
  case kSET_DB:
    return CheckDBMeta(set_db_, type, pattern);
  case kZSET_DB:
    return CheckDBMeta(zset_db_, type, pattern);
  default:
    return Status::InvalidArgument("error db type");
  }
}

Status Nemo::ChecknRecover(DBType type, const std::string& key) {
  switch (type) {
  case kHASH_DB :
    return HChecknRecover(key);
  case kLIST_DB :
    return LChecknRecover(key);
  case kSET_DB :
    return SChecknRecover(key);
  case kZSET_DB :
    return ZChecknRecover(key);
  default:
    return Status::InvalidArgument("error db type");
  }
}

Status Nemo::CheckDBMeta(std::unique_ptr<rocksdb::DBWithTTL> &db, DBType type, const std::string& pattern) {
  // Get Current Snapshot
  const rocksdb::Snapshot* psnap;
  psnap = db->GetSnapshot();
  if (psnap == nullptr) {
    return Status::Corruption("GetSnapshot failed");
  }
  
  // ScanMetas to get all keys + metas 
  std::vector<std::string> keys;
  Status s = ScanKeys(db, psnap, GetMetaPrefix(type), pattern, keys);
  if (!s.ok()) {
    return s;
  }

  // Check and Recover
  MetaPtr pmeta;
  std::vector<std::string>::iterator it = keys.begin();
  for (; it != keys.end(); ++it) {
    s = ChecknRecover(type, *it);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

} 
