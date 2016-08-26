#include <cstdlib>
#include <ctime>

#include "nemo_set.h"
#include "nemo_mutex.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::SGetMetaByKey(const std::string& key, SetMeta& meta) {
  std::string meta_val, meta_key = EncodeSSizeKey(key);
  Status s = set_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
  if (!s.ok()) {
    return s;
  }
  meta.DecodeFrom(meta_val);
  return Status::OK();
}

Status Nemo::SChecknRecover(const std::string& key) {
  RecordLock l(&mutex_set_record_, key);
  SetMeta meta;
  Status s = SGetMetaByKey(key, meta);
  if (!s.ok()) {
    return s;
  }
  // Generate prefix
  std::string key_start = EncodeSetKey(key, "");
  // Iterater and cout
  int field_count = 0;
  rocksdb::Iterator *it;
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = set_db_->GetSnapshot();
  iterate_options.fill_cache = false;
  it = set_db_->NewIterator(iterate_options);
  it->Seek(key_start);
  std::string dbkey, dbfield;
  while (it->Valid()) {
    if ((it->key())[0] != DataType::kSet) {
      break;
    }
    DecodeSetKey(it->key(), &dbkey, &dbfield);
    if (dbkey != key) {
      break;
    }
    ++field_count;
    it->Next();
  }
  set_db_->ReleaseSnapshot(iterate_options.snapshot);
  delete it;
  // Compare
  if (meta.len == field_count) {
    return Status::OK();
  }
  // Fix if needed
  rocksdb::WriteBatch writebatch;
  if (IncrSSize(key, (field_count - meta.len), writebatch) == -1) {
    return Status::Corruption("fix set meta error");
  }
  return set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
}

Status Nemo::SAdd(const std::string &key, const std::string &member, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    RecordLock l(&mutex_set_record_, key);
    //MutexLock l(&mutex_set_);
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.IsNotFound()) { // not found
        *res = 1;
        if (IncrSSize(key, 1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Put(set_key, rocksdb::Slice());
    } else if (s.ok()) {
        *res = 0;
    } else {
        return Status::Corruption("sadd check member error");
    }

    s = set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

Status Nemo::SAddNoLock(const std::string &key, const std::string &member, int64_t *res) {
    Status s;
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.IsNotFound()) { // not found
        *res = 1;
        if (IncrSSize(key, 1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Put(set_key, rocksdb::Slice());
    } else if (s.ok()) {
        *res = 0;
    } else {
        return Status::Corruption("sadd check member error");
    }

    s = set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

Status Nemo::SRem(const std::string &key, const std::string &member, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    //MutexLock l(&mutex_set_);
    RecordLock l(&mutex_set_record_, key);
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.ok()) {
        *res = 1;
        if (IncrSSize(key, -1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Delete(set_key);
        s = set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return Status::Corruption("srem check member error");
    }
    return s;
}

Status Nemo::SRemNoLock(const std::string &key, const std::string &member, int64_t *res) {
    Status s;
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.ok()) {
        *res = 1;
        if (IncrSSize(key, -1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Delete(set_key);
        s = set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return Status::Corruption("srem check member error");
    }
    return s;
}

int Nemo::IncrSSize(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch) {
    int64_t len = SCard(key);

    if (len == -1) {
        return -1;
    }

    std::string size_key = EncodeSSizeKey(key);

    len += incr;
    writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));

   // if (len == 0) {
   //     writebatch.Delete(size_key);
   // } else {
   //     writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
   // }
    return 0;
}

int64_t Nemo::SCard(const std::string &key) {
    std::string size_key = EncodeSSizeKey(key);
    std::string val;
    Status s;

    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        return 0;
    } else if(!s.ok()) {
        return -1;
    } else {
        if (val.size() != sizeof(int64_t)) {
            return 0;
        }
        int64_t ret = *(int64_t *)val.data();
        return ret < 0 ? 0 : ret;
    }
}

SIterator* Nemo::SScan(const std::string &key, uint64_t limit, bool use_snapshot) {
    std::string set_key = EncodeSetKey(key, "");

    rocksdb::ReadOptions read_options;
    if (use_snapshot) {
        read_options.snapshot = set_db_->GetSnapshot();
    }
    read_options.fill_cache = false;

    rocksdb::Iterator *it = set_db_->NewIterator(read_options);
    it->Seek(set_key);

    IteratorOptions iter_options("", limit, read_options);

    return new SIterator(it, iter_options, key); 
}

Status Nemo::SMembers(const std::string &key, std::vector<std::string> &members) {
    SIterator *iter = SScan(key, -1, true);

    for (; iter->Valid(); iter->Next()) {
        members.push_back(iter->member());
    }
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::SUnion(const std::vector<std::string> &keys, std::vector<std::string>& members) {
    std::map<std::string, bool> result_flag;
    
    for (int i = 0; i < (int)keys.size(); i++) {
        RecordLock l(&mutex_set_record_, keys[i]);
        SIterator *iter = SScan(keys[i], -1, true);
        
        for (; iter->Valid(); iter->Next()) {
            std::string member = iter->member();
            if (result_flag.find(member) == result_flag.end()) {
                members.push_back(member);
                result_flag[member] = 1;
            }
        }
        set_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }
    return Status::OK();
}

Status Nemo::SUnionStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res) {

    int numkey = keys.size();
    //MutexLock l(&mutex_set_);
    if (numkey <= 0) {
        return Status::InvalidArgument("invalid parameter, no keys");
    }

    std::map<std::string, int> member_result;
    std::map<std::string, int>::iterator it;

    for (int i = 0; i < numkey; i++) {
        RecordLock l(&mutex_set_record_, keys[i]);
        SIterator *iter = SScan(keys[i], -1, true);
        
        for (; iter->Valid(); iter->Next()) {
            member_result[iter->member()] = 1;
        }
        set_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }

    // we delete the destination if it exists
    Status s;
    int64_t tmp_res;

    RecordLock l(&mutex_set_record_, destination);
    if (SCard(destination) > 0) {
        SIterator *iter = SScan(destination, -1, true);
        for (; iter->Valid(); iter->Next()) {
            s = SRemNoLock(destination, iter->member(), &tmp_res);
            if (!s.ok()) {
                delete iter;
                return s;
            }
        }
        set_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }

    for (it = member_result.begin(); it != member_result.end(); it++) {
        s = SAddNoLock(destination, it->first, &tmp_res);
        if (!s.ok()) {
            return s;
        }
    }
    *res = member_result.size();
    return Status::OK();
}

bool Nemo::SIsMember(const std::string &key, const std::string &member) {
    std::string val;

    std::string set_key = EncodeSetKey(key, member);
    Status s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    return s.ok();
}

//Note: no lock
Status Nemo::SInter(const std::vector<std::string> &keys, std::vector<std::string>& members) {
    int numkey = keys.size();
    if (numkey <= 0) {
        return Status::InvalidArgument("SInter invalid parameter, no keys");
    }

    SIterator *iter = SScan(keys[0], -1, true);
    
    for (; iter->Valid(); iter->Next()) {
        int i = 1;
        std::string member = iter->member();
        for (; i < numkey; i++) {
            if (!SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            members.push_back(member);
        }
    }
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::SInterStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res) {
    int numkey = keys.size();
    //MutexLock l(&mutex_set_);
    if (numkey <= 0) {
        return Status::Corruption("SInter invalid parameter, no keys");
    }

    for (size_t i = 0; i < keys.size(); i++) {
      mutex_set_record_.Lock(keys[i]);
    }

    std::map<std::string, int> member_result;
    std::map<std::string, int>::iterator it;
    SIterator *iter = SScan(keys[0], -1, true);

    for (; iter->Valid(); iter->Next()) {
        int i = 1;
        std::string member = iter->member();
        for (; i < numkey; i++) {
            if (!SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            member_result[member] = 1;
        }
    }
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;

    for (size_t i = 0; i < keys.size(); i++) {
      mutex_set_record_.Unlock(keys[i]);
    }

    // we delete the destination if it exists
    Status s;
    int64_t tmp_res;

    RecordLock l(&mutex_set_record_, destination);
    if (SCard(destination) > 0) {
        SIterator *iter = SScan(destination, -1, true);
        for (; iter->Valid(); iter->Next()) {
            s = SRemNoLock(destination, iter->member(), &tmp_res);
            if (!s.ok()) {
                delete iter;
                return s;
            }
        }
        set_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }

    for (it = member_result.begin(); it != member_result.end(); it++) {
        s = SAddNoLock(destination, it->first, &tmp_res);
        if (!s.ok()) {
            break;
        }
    }

    *res = member_result.size();
    return s;
}

Status Nemo::SDiff(const std::vector<std::string> &keys, std::vector<std::string>& members) {
    int numkey = keys.size();
    if (numkey <= 0) {
        return Status::Corruption("SDiff invalid parameter, no keys");
    }

    SIterator *iter = SScan(keys[0], -1, true);
    
    for (; iter->Valid(); iter->Next()) {
        int i = 1;
        std::string member = iter->member();
        for (; i < numkey; i++) {
            if (SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            members.push_back(member);
        }
    }
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::SDiffStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res) {
    int numkey = keys.size();
    //MutexLock l(&mutex_set_);
    if (numkey <= 0) {
        return Status::Corruption("SDiff invalid parameter, no keys");
    }

    std::map<std::string, int> member_result;
    std::map<std::string, int>::iterator it;

    SIterator *iter = SScan(keys[0], -1, true);

    for (; iter->Valid(); iter->Next()) {
        int i = 1;
        std::string member = iter->member();
        for (; i < numkey; i++) {
            if (SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            member_result[member] = 1;
        }
    }
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;

    // we delete the destination if it exists
    Status s;
    int64_t tmp_res;

    RecordLock l(&mutex_set_record_, destination);
    if (SCard(destination) > 0) {
        SIterator *iter = SScan(destination, -1, true);
        for (; iter->Valid(); iter->Next()) {
            s = SRemNoLock(destination, iter->member(), &tmp_res);
            if (!s.ok()) {
                delete iter;
                return s;
            }
        }
        set_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }


    for (it = member_result.begin(); it != member_result.end(); it++) {
        s = SAddNoLock(destination, it->first, &tmp_res);
        if (!s.ok()) {
            return s;
        }
    }
    *res = member_result.size();
    return Status::OK();
}

int64_t Nemo::AddAndGetSpopCount(const std::string &key) {
  std::map<std::string, int64_t> &spop_counts_map = spop_counts_store_.map_;
  std::list<std::string> &spop_counts_list = spop_counts_store_.list_;
  std::map<std::string, int64_t>::iterator iter;
  int64_t spop_count;
  MutexLock lm(&mutex_spop_counts_);
  iter = spop_counts_map.find(key);
  if (iter != spop_counts_map.end()) {
    spop_count = ++iter->second;
  } else {
    spop_count = ++spop_counts_map[key];
    spop_counts_list.push_back(key);
    if (spop_counts_store_.cur_size_ >= spop_counts_store_.max_size_) {
      spop_counts_map.erase(spop_counts_list.front());
      spop_counts_list.erase(spop_counts_list.begin());
    } else {
      ++spop_counts_store_.cur_size_;
    } 
  } 
  return spop_count; 
}

static inline int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void Nemo::ResetSpopCount(const std::string &key) {
  MutexLock lm(&mutex_spop_counts_);
  std::map<std::string, int64_t>::iterator iter = spop_counts_store_.map_.find(key);
  if (iter == spop_counts_store_.map_.end()) {
    return;
  }
  iter->second = 0;
  return;
}

Status Nemo::SPop(const std::string &key, std::string &member) {
#define SPOP_COMPACT_THRESHOLD_COUNT 500
    int card = SCard(key);
    if (card <= 0) {
        return Status::NotFound();
    }

    //MutexLock l(&mutex_set_);
    uint64_t start_us = NowMicros(), duration_us;
    srand (start_us);
    int k = rand() % (card < 100 ? card : 100 ) + 1;
    RecordLock l(&mutex_set_record_, key);
 
    SIterator *iter = SScan(key, -1, true);
    for (int i = 0; i < k - 1; i++) {
        iter->Next();
    }
    member = iter->member();
    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
   
    duration_us = NowMicros() - start_us;
    int64_t spop_count = AddAndGetSpopCount(key); 
    if (spop_count >= SPOP_COMPACT_THRESHOLD_COUNT) {
      AddBGTask({DBType::kSET_DB, OPERATION::kDEL_KEY, key, ""/*not used*/}); 
      ResetSpopCount(key);
    } else if (duration_us > 1000000UL) {
      AddBGTask({DBType::kSET_DB, OPERATION::kDEL_KEY, key, ""/*not used*/}); 
    }
    int64_t res;
    return SRemNoLock(key, member, &res);
}

//Note: no lock
Status Nemo::SRandMember(const std::string &key, std::vector<std::string> &members, const int count) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    members.clear();

    if (count == 0) {
        return Status::OK();
    }

    int card = SCard(key);
    if (card <= 0) {
        return Status::NotFound();
    }

    std::map<int, int> idx_flag;
    bool repeat_flag = false;
    int remain_card = card;
    int ncount = count;
    if (ncount < 0) {
        repeat_flag = true;
        ncount = -ncount;
    }

    srand (time(NULL));
    for (int ci = 0; ci < ncount; ci++) {
        if (repeat_flag) {
            int k = rand() % card;
            idx_flag[k]++;
        } else {
            if (remain_card <= 0) break;

            // everytime, we get the k-th remain idx
            int k = rand() % remain_card + 1;
            int i = 0;
            int cnt = 0;  // the valid k-th number
            while (i < card && cnt < k) {
                if (idx_flag.find(i) == idx_flag.end()) {
                    cnt++;
                }

                if (cnt == k) break;
                i++;
            }
            idx_flag[i]++;
            remain_card--;
        }
    }

    SIterator *iter = SScan(key, -1, true);
    for (int i = 0, cnt = 0; iter->Valid() && cnt < ncount; iter->Next(), i++) {
        if (idx_flag.find(i) != idx_flag.end()) {
            for (int j = 0; j < idx_flag[i]; j++) {
                members.push_back(iter->member());
                cnt++;
            }
        }
    }

    set_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::SMove(const std::string &source, const std::string &destination, const std::string &member, int64_t *res) {
    if (source.size() >= KEY_MAX_LENGTH || source.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    Status s;

    if (source == destination) {
      *res = 1;
      return Status::OK();
    }

    //MutexLock l(&mutex_set_);
    rocksdb::WriteBatch writebatch;
    std::string source_key = EncodeSetKey(source, member);
    std::string destination_key = EncodeSetKey(destination, member);

    RecordLock l1(&mutex_set_record_, source);
    RecordLock l2(&mutex_set_record_, destination);
    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), source_key, &val);

    if (s.ok()) {
        *res = 1;
        if (source != destination) {
          if (IncrSSize(source, -1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
          }
          writebatch.Delete(source_key);

          s = set_db_->Get(rocksdb::ReadOptions(), destination_key, &val);
          if (s.IsNotFound()) {
            if (IncrSSize(destination, 1, writebatch) < 0) {
              return Status::Corruption("incrSSize error");
            }
          }
          writebatch.Put(destination_key, rocksdb::Slice());

          s = set_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
        }
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return Status::Corruption("srem check member error");
    }
    return s;
}

Status Nemo::SDelKey(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;
    *res = 0;

    std::string size_key = EncodeSSizeKey(key);

    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        s = Status::NotFound("");
      } else {
        len = 0;
        *res = 1;
        //MutexLock l(&mutex_set_);
        s = set_db_->PutWithKeyVersion(rocksdb::WriteOptions(), size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
      }
    }
    return s;
}

Status Nemo::SExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_set_record_, key);
    std::string size_key = EncodeSSizeKey(key);
    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("");
      }

      if (seconds > 0) {
        //MutexLock l(&mutex_set_);
        s = set_db_->PutWithKeyTTL(rocksdb::WriteOptions(), size_key, val, seconds);
      } else { 
        int64_t count;
        s = SDelKey(key, &count);
      }
      *res = 1;
    }
    return s;
}

Status Nemo::STTL(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;
    RecordLock l(&mutex_set_record_, key);

    std::string size_key = EncodeSSizeKey(key);
    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        int32_t ttl;
        s = set_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        *res = ttl;
    }
    return s;
}

Status Nemo::SPersist(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_set_record_, key);

    *res = 0;
    std::string size_key = EncodeSSizeKey(key);
    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);

    if (s.ok()) {
        int32_t ttl;
        s = set_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        if (s.ok() && ttl >= 0) {
            //MutexLock l(&mutex_set_);
            s = set_db_->Put(rocksdb::WriteOptions(), size_key, val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::SExpireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_set_record_, key);

    std::string size_key = EncodeSSizeKey(key);
    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("");
      }

      std::time_t cur = std::time(0);
      if (timestamp <= cur) {
        int64_t count;
        s = SDelKey(key, &count);
      } else {
        //MutexLock l(&mutex_set_);
        s = set_db_->PutWithExpiredTime(rocksdb::WriteOptions(), size_key, val, timestamp);
      }
      *res = 1;
    }
    return s;
}

