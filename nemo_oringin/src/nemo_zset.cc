#include <algorithm>
#include <climits>
#include <ctime>

#include "nemo_zset.h"
#include "nemo_mutex.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"
using namespace nemo;

// Iterator kZScore and Dress kZScore for kZSet
Status Nemo::ZDressZScoreforZSet(const std::string& key, int *count) {
  std::string key_start = EncodeZScorePrefix(key);
  
  rocksdb::Iterator *it;
  rocksdb::WriteBatch writebatch;
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = zset_db_->GetSnapshot();
  iterate_options.fill_cache = false;
  it = zset_db_->NewIterator(iterate_options);
  it->Seek(key_start);
  std::string dbkey, dbfield, zset_key, val;
  Status s = Status::OK();
  while (it->Valid()) {
    if ((it->key())[0] != DataType::kZScore) {
      break;
    }
    double zscore_score = 0.0;
    DecodeZScoreKey(it->key(), &dbkey, &dbfield, &zscore_score);
    if (dbkey != key) {
      break;
    }
    // Look up in kZSet
    zset_key = EncodeZSetKey(key, dbfield);
    s = zset_db_->Get(rocksdb::ReadOptions(), zset_key, &val);
    double zset_score = *((double *)val.data());
    if (s.ok()) {
      if (fabs(zset_score - zscore_score) > eps) {
        // TODO log inconsistent
        // Change score in ZScore
        writebatch.Delete(it->key());
        std::string new_key = EncodeZScoreKey(key, dbfield, zset_score);
        writebatch.Put(new_key, "");
      }
      ++(*count);
    } else if (s.IsNotFound()) {
      // TODO log inconsistent
      writebatch.Delete(it->key());
    } else {
      break;
    }
    it->Next();
  }
  zset_db_->ReleaseSnapshot(iterate_options.snapshot);
  delete it;
  if (writebatch.Count() != 0) {
    s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
  }
  return s;
}

// Iterator kZSet and Dress kZSet for kZScore
Status Nemo::ZDressZSetforZScore(const std::string& key, int *count) {
  std::string key_start = EncodeZSetKey(key, "");
  
  rocksdb::Iterator *it;
  rocksdb::WriteBatch writebatch;
  rocksdb::ReadOptions iterate_options;
  iterate_options.snapshot = zset_db_->GetSnapshot();
  iterate_options.fill_cache = false;
  it = zset_db_->NewIterator(iterate_options);
  it->Seek(key_start);
  std::string dbkey, dbfield, score_key, val;
  double zset_score = 0.0;
  Status s;
  while (it->Valid()) {
    if ((it->key())[0] != DataType::kZSet) {
      break;
    }
    DecodeZSetKey(it->key(), &dbkey, &dbfield);
    if (dbkey != key) {
      break;
    }
    // Look up in kZScore
    zset_score = *((double *)(it->value().data()));
    score_key = EncodeZScoreKey(key, dbfield, zset_score);
    s = zset_db_->Get(rocksdb::ReadOptions(), score_key, &val);
    if (s.ok()) {
      ++(*count);
    } else if (s.IsNotFound()) {
      // TODO log inconsistent
      writebatch.Delete(it->key());
    } else {
      break;
    }
    it->Next();
  }
  zset_db_->ReleaseSnapshot(iterate_options.snapshot);
  delete it;
  if (writebatch.Count() != 0) {
    s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
  }
  return s;
}

Status Nemo::ZGetMetaByKey(const std::string& key, ZSetMeta& meta) {
  std::string meta_val, meta_key = EncodeZSizeKey(key);
  Status s = zset_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
  if (!s.ok()) {
    return s;
  }
  meta.DecodeFrom(meta_val);
  return Status::OK();
}

Status Nemo::ZChecknRecover(const std::string& key) {
  RecordLock l(&mutex_zset_record_, key);
  ZSetMeta meta;
  Status s = ZGetMetaByKey(key, meta);
  if (!s.ok()) {
    return s;
  }
  // Iterator y and dress for z
  int field_count = 0;
  s = ZDressZScoreforZSet(key, &field_count);
  if (!s.ok()) {
    return s;
  }
  field_count = 0;
  // Iterator z and dress for y
  s = ZDressZSetforZScore(key, &field_count);
  if (!s.ok()) {
    return s;
  }
  // Compare
  if (meta.len == field_count) {
    return Status::OK();
  }
  // Fix if needed
  rocksdb::WriteBatch writebatch;
  if (IncrZLen(key, (field_count - meta.len), writebatch) == -1) {
    return Status::Corruption("fix zset meta error");
  }
  return zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &(writebatch));
}

Status Nemo::ZAdd(const std::string &key, const double score, const std::string &member, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    if (score < ZSET_SCORE_MIN || score > ZSET_SCORE_MAX) {
       return Status::InvalidArgument("score overflow");
    }
    if (key.size() >= KEY_MAX_LENGTH) {
       return Status::InvalidArgument("Invalid key length");
    }

    //std::string db_key = EncodeZSetKey(key, member);
    //std::string size_key = EncodeZSizeKey(key);
    //std::string score_key = EncodeZScoreKey(key, member, score); 
    rocksdb::WriteBatch batch;
    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, key);
    int ret = DoZSet(key, score, member, batch);
    if (ret == 2) {
        if (IncrZLen(key, 1, batch) == 0) {
            s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            *res = 1;
            return s;
        } else {
            return Status::Corruption("incr zsize error");
        }
    } else if (ret == 1) {
        *res = 0;
        s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        return s;
    } else if (ret == 0) {
        *res = 0;
        return Status::OK();
    } else {
        return Status::Corruption("zadd error");
    }
}

Status Nemo::ZAddNoLock(const std::string &key, const double score, const std::string &member, int64_t *res) {
    Status s;
    if (score < ZSET_SCORE_MIN || score > ZSET_SCORE_MAX) {
        return Status::Corruption("zset score overflow");
    }

    //std::string db_key = EncodeZSetKey(key, member);
    //std::string size_key = EncodeZSizeKey(key);
    //std::string score_key = EncodeZScoreKey(key, member, score); 
    rocksdb::WriteBatch batch;
    int ret = DoZSet(key, score, member, batch);
    if (ret == 2) {
        if (IncrZLen(key, 1, batch) == 0) {
            s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            *res = 1;
            return s;
        } else {
            return Status::Corruption("incr zsize error");
        }
    } else if (ret == 1) {
        *res = 0;
        s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        return s;
    } else if (ret == 0) {
        *res = 0;
        return Status::OK();
    } else {
        return Status::Corruption("zadd error");
    }
}

int64_t Nemo::ZCard(const std::string &key) {
    Status s;
    std::string val;
    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.ok()) {
        if (val.size() != sizeof(uint64_t)) {
            return 0;
        }
        int64_t ret = *(int64_t *)val.data();
        return ret < 0? 0 : ret;
    } else if (s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

ZIterator* Nemo::ZScan(const std::string &key, const double begin, const double end, uint64_t limit, bool use_snapshot) {
    double rel_begin = begin;
    double rel_end = end + eps;
    if (begin < ZSET_SCORE_MIN) {
      rel_begin = ZSET_SCORE_MIN;
    }
    if (end > ZSET_SCORE_MAX) {
      rel_end = ZSET_SCORE_MAX;
    }
    std::string key_start, key_end;
    key_start = EncodeZScoreKey(key, "", rel_begin);
    key_end = EncodeZScoreKey(key, "", rel_end);
    rocksdb::ReadOptions read_options;
    if (use_snapshot) {
        read_options.snapshot = zset_db_->GetSnapshot();
    }
    read_options.fill_cache = false;

    IteratorOptions iter_options(key_end, limit, read_options);

    rocksdb::Iterator *it = zset_db_->NewIterator(read_options);
    it->Seek(key_start);
    return new ZIterator(it, iter_options, key); 
}

ZLexIterator* Nemo::ZScanbylex(const std::string &key, const std::string &min, const std::string &max, uint64_t limit, bool use_snapshot) {
    std::string key_start, key_end;
    key_start = EncodeZSetKey(key, min);
    if (max == "") {
        key_end == "";
    } else {
        key_end = EncodeZSetKey(key, max);
    }

    rocksdb::ReadOptions read_options;
    if (use_snapshot) {
        read_options.snapshot = zset_db_->GetSnapshot();
    }
    read_options.fill_cache = false;

    IteratorOptions iter_options(key_end, limit, read_options);

    rocksdb::Iterator *it = zset_db_->NewIterator(read_options);
    it->Seek(key_start);
    return new ZLexIterator(it, iter_options, key); 
}

int64_t Nemo::ZCount(const std::string &key, const double begin, const double end, bool is_lo, bool is_ro) {
    double b = is_lo ? begin + eps : begin;
    double e = is_ro ? end - eps : end;
//    MutexLock l(&mutex_zset_);
    ZIterator* it = ZScan(key, b, e, -1, true);
    double s;
    int64_t n = 0;
    for (; it->Valid(); it->Next()) {
        s = it->score();
        if (s >= begin && s <= end ) {
            n++;
        } else {
            break;
        }
    }
    zset_db_->ReleaseSnapshot(it->read_options().snapshot);
    delete it;
    return n;
}

Status Nemo::ZIncrby(const std::string &key, const std::string &member, const double by, std::string &new_score) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);
    rocksdb::WriteBatch writebatch;
    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, key);

    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    double dval;
    if (s.ok()) {
        dval = *((double *)old_score.data());
        score_key = EncodeZScoreKey(key, member, dval);
        writebatch.Delete(score_key);

        dval += by;
        if (dval < ZSET_SCORE_MIN || dval > ZSET_SCORE_MAX) {
            return Status::Corruption("zset score overflow");
        }
        score_key = EncodeZScoreKey(key, member, dval);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&dval), sizeof(double));
        writebatch.Put(db_key, buf);
    } else if (s.IsNotFound()) {
        if (by < ZSET_SCORE_MIN || by > ZSET_SCORE_MAX) {
            return Status::Corruption("zset score overflow");
        }
        dval = by;
        score_key = EncodeZScoreKey(key, member, by);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&by), sizeof(double));
        writebatch.Put(db_key, buf);
        if (IncrZLen(key, 1, writebatch) != 0) {
            return Status::Corruption("incr zsize error");
        }
    } else {
        return Status::Corruption("get the key error");
    }
    std::string res = std::to_string(dval); 
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_score = res.substr(0, pos); 
    if (new_score[new_score.size()-1] == '.') {
        new_score = new_score.substr(0, new_score.size()-1);
    }
    s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &writebatch);
    return s;
}

Status Nemo::ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
//    MutexLock l(&mutex_zset_);
    int64_t t_size = ZCard(key);
    if (t_size >= 0) {
        int64_t t_start = start >= 0 ? start : t_size + start;
        int64_t t_stop = stop >= 0 ? stop : t_size + stop;
        if (t_start < 0) {
            t_start = 0;
        }
        if (t_stop > t_size - 1) {
            t_stop = t_size - 1;
        }
        if (t_start > t_stop || t_start > t_size - 1 || t_stop < 0) {
            return Status::OK();
        } else {
            int n = 0;
            ZIterator* iter = NULL;
            if (t_size > 1000 && t_start > t_size / 2) {
              std::string zscore_key_end = EncodeZScoreKey(key, "", ZSET_SCORE_MAX);
              //std::string zscore_key_start = EncodeZScoreKey(key, "", ZSET_SCORE_MIN);
              std::string zscore_key_start = "";
              rocksdb::ReadOptions read_options;
              read_options.snapshot = zset_db_->GetSnapshot();
              read_options.fill_cache = false;
              IteratorOptions iter_options(zscore_key_start, -1, read_options, kBackward);
              rocksdb::Iterator* rocksdb_it = zset_db_->NewIterator(read_options);
              rocksdb_it->Seek(zscore_key_end);
              rocksdb_it->Prev();
              iter = new ZIterator(rocksdb_it, iter_options, key);
              n = t_size - 1;
              for (; n > t_stop && iter->Valid(); iter->Next(), n--);
              if (n != t_stop) {
                zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
                delete iter;
                return Status::Corruption("ziterate error");
              }
              sms.resize(t_stop - t_start + 1);
              for (; n >= t_start && iter->Valid(); n--) {
                sms[n - t_start] = {iter->score(), iter->member()};
                iter->Next();
              }
            } else {
              iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1, true);
              if (iter == NULL) {
                return Status::Corruption("zscan error");
              }
              n = 0;
              for (; n < t_start && iter->Valid(); iter->Next(), n++);

              if (n < t_start) {
                  zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
                  delete iter;
                  return Status::Corruption("ziterate error");
              }
              for (; n <= t_stop && iter->Valid(); iter->Next(), n++) {
                  sms.push_back({iter->score(), iter->member()});
              }
            }
            zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
            delete iter;
            return Status::OK();
        }
    } else {
        return Status::Corruption("get zsize error");
    }
}

Status Nemo::ZRangebyscore(const std::string &key, const double mn, const double mx, std::vector<SM> &sms, bool is_lo, bool is_ro) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    double start = is_lo ? mn + eps : mn;
    double stop = is_ro ? mx - eps : mx;
//    MutexLock l(&mutex_zset_);
    ZIterator *iter = ZScan(key, start, stop, -1, true);
    for (; iter->Valid(); iter->Next()) {
        sms.push_back({iter->score(), iter->member()});
    }
    zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::ZUnionStore(const std::string &destination, const int numkeys, const std::vector<std::string>& keys, const std::vector<double>& weights = std::vector<double>(), Aggregate agg = SUM, int64_t *res = 0) {
    std::map<std::string, double> mp_member_score;
    *res = 0;

    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, destination);

    for (int key_i = 0; key_i < numkeys; key_i++) {
      mutex_set_record_.Lock(keys[key_i]);
    }

    int weights_size = static_cast<int>(weights.size());
    for (int key_i = 0; key_i < numkeys; key_i++) {
        ZIterator *iter = ZScan(keys[key_i], ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
        for (; iter->Valid(); iter->Next()) {
            std::string member = iter->member();

            double weight = 1;
            if (weights_size > key_i) {
                weight = weights[key_i];
            }

            if (mp_member_score.find(member) == mp_member_score.end()) {
                mp_member_score[member] = weight * iter->score();
            } else {
                double score = mp_member_score[member];
                switch (agg) {
                  case SUM: score += weight * iter->score(); break;
                  case MIN: score = std::min(score, weight * iter->score()); break;
                  case MAX: score = std::max(score, weight * iter->score()); break;
                }
                mp_member_score[member] = score;
            }
        }
        
        delete iter;
    }

    int64_t add_ret;
    int64_t rem_ret;
    std::map<std::string, double>::iterator it;
    Status status;

    status = ZRemrangebyrankNoLock(destination, 0, -1, &rem_ret);

    for (it = mp_member_score.begin(); it != mp_member_score.end(); it++) {
        status = ZAddNoLock(destination, it->second, it->first, &add_ret);
        if (!status.ok()) {
            break;
        }
    }

    for (int key_i = 0; key_i < numkeys; key_i++) {
      mutex_set_record_.Unlock(keys[key_i]);
    }

    *res = mp_member_score.size();
    return Status::OK();
}

Status Nemo::ZInterStore(const std::string &destination, const int numkeys, const std::vector<std::string>& keys, const std::vector<double>& weights = std::vector<double>(), Aggregate agg = SUM, int64_t *res = 0) {
    Status s;
    *res = 0;

    if (numkeys < 2) {
        return Status::OK();
    }

    std::string l_key = keys[0];
    std::string old_score;
    std::string member;
    std::string db_key;
    int64_t add_ret;
    std::map<std::string, double> mp_member_score;
    double l_weight = 1;
    double r_weight = 1;
    double l_score;
    int key_i;

    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, destination);

    for (int key_i = 0; key_i < numkeys; key_i++) {
      mutex_set_record_.Lock(keys[key_i]);
    }

    int weights_size = static_cast<int>(weights.size());
    if (weights_size > 1) {
        l_weight = weights[0];
    }

    ZIterator *iter = ZScan(l_key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
    
    for (; iter != NULL && iter->Valid(); iter->Next()) {
        member = iter->member();
        l_score = l_weight * iter->score();

        for (key_i = 1; key_i < numkeys; key_i++) {
          r_weight = 1;
          if (weights_size > key_i) {
              r_weight = weights[key_i];
          }
          
          db_key = EncodeZSetKey(keys[key_i], member);
          s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);

          if (s.ok()) {
            double r_score = *((double *)old_score.data());
            switch (agg) {
              case SUM: l_score += r_weight * r_score; break;
              case MIN: l_score = std::min(l_score, r_weight * r_score); break;
              case MAX: l_score = std::max(l_score, r_weight * r_score); break;
            }
          } else if (s.IsNotFound()) {
              break;
          } else {
              delete iter;
              return s;
          }
        }

        if (key_i >= numkeys) {
            mp_member_score[member] = l_score;
        }
    }
    delete iter;

    int64_t rem_ret;
    std::map<std::string, double>::iterator it;

    s = ZRemrangebyrankNoLock(destination, 0, -1, &rem_ret);

    for (it = mp_member_score.begin(); it != mp_member_score.end(); it++) {
        s = ZAddNoLock(destination, it->second, it->first, &add_ret);
        if (!s.ok()) {
           break; 
        }
        (*res)++;
    }

    for (int key_i = 0; key_i < numkeys; key_i++) {
      mutex_set_record_.Unlock(keys[key_i]);
    }

    return Status::OK();
}

Status Nemo::ZRem(const std::string &key, const std::string &member, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    *res = 0;
    rocksdb::WriteBatch batch;
    std::string old_score;

    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, key);

    std::string db_key = EncodeZSetKey(key, member);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);

    if (s.ok()) {
      batch.Delete(db_key);

      double dscore = *((double *)old_score.data());
      std::string score_key = EncodeZScoreKey(key, member, dscore);
      batch.Delete(score_key);

      if (IncrZLen(key, -1, batch) == 0) {
        s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        *res = 1;
        return s;
      } else {
        return Status::Corruption("incr zsize error");
      }
    } else {
      return s;
    }
}

Status Nemo::ZRank(const std::string &key, const std::string &member, int64_t *rank) {
    Status s;
    *rank = 0;
    std::string old_score;

//    MutexLock l(&mutex_zset_);

    std::string db_key = EncodeZSetKey(key, member);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    int64_t count = 0;
    if (s.ok()) {
        ZIterator *iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1, true);
        for (; iter->Valid() && iter->member().compare(member) != 0; iter->Next()) {
            count++;
        }
        if (iter->member().compare(member) == 0) {
            *rank = count;
        }
        zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
    }
    return s;
}

Status Nemo::ZRevrank(const std::string &key, const std::string &member, int64_t *rank) {
    Status s;
    *rank = 0;
    std::string old_score;

//    MutexLock l(&mutex_zset_);

    std::string db_key = EncodeZSetKey(key, member);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    int64_t count = -1;
    if (s.ok()) {
        ZIterator *iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1, true);
        for (; iter->Valid() && iter->member().compare(member) != 0; iter->Next()) {
        }
        if (iter->member().compare(member) == 0) {
            for (; iter->Valid(); iter->Next()) {
                count++;
            }
        } 
        zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
        delete iter;
        *rank = count;
    }
    return s;
}

Status Nemo::ZScore(const std::string &key, const std::string &member, double *score) {
    Status s;
    *score = 0;
    std::string str_score;

    std::string db_key = EncodeZSetKey(key, member);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &str_score);
    if (s.ok()) {
        *score = *((double *)(str_score.data()));
    }
    return s;
}

Status Nemo::ZRangebylex(const std::string &key, const std::string &min, const std::string &max, std::vector<std::string> &members) {
//    MutexLock l(&mutex_zset_);
    ZLexIterator *iter = ZScanbylex(key, min, max, -1, true);
    for (; iter->Valid(); iter->Next()) {
        members.push_back(iter->member());
    }
    zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::ZLexcount(const std::string &key, const std::string &min, const std::string &max, int64_t* count) {
//    MutexLock l(&mutex_zset_);
    *count = 0;
    ZLexIterator *iter = ZScanbylex(key, min, max, -1, true);
    for (; iter->Valid(); iter->Next()) {
        (*count)++;
    }
    zset_db_->ReleaseSnapshot(iter->read_options().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::ZRemrangebylex(const std::string &key, const std::string &min, const std::string &max, bool is_lo, bool is_ro, int64_t* count) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    *count = 0;
    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, key);

    ZLexIterator *iter = ZScanbylex(key, min, max, -1);
    rocksdb::WriteBatch batch;
    std::string score_key;
    std::string old_score;
    std::string size_key;
    std::string db_key;
    std::string member;
    Status s;
    double dscore;
    if (iter->Valid()) {
        member = iter->member();
        if (min == "" || (!is_lo && member.compare(min) == 0)) {
            db_key = EncodeZSetKey(key, member);
            s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
            if (s.ok()) {
              batch.Delete(db_key);
              dscore = *((double *)old_score.data());
              score_key = EncodeZScoreKey(key, member, dscore);
              batch.Delete(score_key);
              (*count)++;
            } else {
                delete iter;
                return s;
            }
        }
    }
    if (min == "" || member.compare(min) == 0) {
      iter->Next();
    }
    for (; iter->Valid(); iter->Next()) {
        member = iter->member();
        if (max == "" || member.compare(max) < 0) {
            db_key = EncodeZSetKey(key, member);
            s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
            if (s.ok()) {
              batch.Delete(db_key);
              dscore = *((double *)old_score.data());
              score_key = EncodeZScoreKey(key, member, dscore);
              batch.Delete(score_key);
              (*count)++;
            } else {
                delete iter;
                return s;
            } 
        } else if (!is_ro && member.compare(max) == 0) {
            db_key = EncodeZSetKey(key, member);
            s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
            if (s.ok()) {
              batch.Delete(db_key);
              dscore = *((double *)old_score.data());
              score_key = EncodeZScoreKey(key, member, dscore);
              batch.Delete(score_key);
              (*count)++;
            } else {
                delete iter;
                return s;
            } 
        }
    }
    delete iter;
    if (IncrZLen(key, -(*count), batch) == 0) {
        s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return Status::Corruption("incr zsize error");
    }
}

Status Nemo::ZRemrangebyrank(const std::string &key, const int64_t start, const int64_t stop, int64_t* count) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    rocksdb::WriteBatch batch;
    std::string score_key;
    std::string size_key;
    std::string db_key;
    *count = 0;
    Status s;
    //MutexLock l(&mutex_zset_);
    RecordLock l(&mutex_zset_record_, key);

    int64_t t_size = ZCard(key);
    if (t_size >= 0) {
        int64_t t_start = start >= 0 ? start : t_size + start;
        int64_t t_stop = stop >= 0 ? stop : t_size + stop;
        if (t_start < 0) {
            t_start = 0;
        }
        if (t_stop > t_size - 1) {
            t_stop = t_size - 1;
        }
        if (t_start > t_stop || t_start > t_size - 1 || t_stop < 0) {
            return Status::OK();
        } else {
            ZIterator *iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
            if (iter == NULL) {
                return Status::Corruption("zscan error");
            }
            int32_t n = 0;
            for (; n < t_start && iter->Valid(); iter->Next(), n++);

            if (n<t_start) {
                delete iter;
                return Status::Corruption("ziterate error");
            } else {
                for (; n <= t_stop && iter->Valid(); iter->Next(), n++) {
                    db_key = EncodeZSetKey(key, iter->member());
                    score_key = EncodeZScoreKey(key, iter->member(), iter->score());
                    batch.Delete(db_key);
                    batch.Delete(score_key);
                    (*count)++;
                }
                delete iter;
                if (IncrZLen(key, -(*count), batch) == 0) {
                    s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
                    return s;
                } else {
                    return Status::Corruption("incr zsize error");
                }
            }
        }
    } else {
        return Status::Corruption("get zsize error");
    }
}

Status Nemo::ZRemrangebyrankNoLock(const std::string &key, const int64_t start, const int64_t stop, int64_t* count) {
    rocksdb::WriteBatch batch;
    std::string score_key;
    std::string size_key;
    std::string db_key;
    *count = 0;
    Status s;
    int64_t t_size = ZCard(key);
    if (t_size >= 0) {
        int64_t t_start = start >= 0 ? start : t_size + start;
        int64_t t_stop = stop >= 0 ? stop : t_size + stop;
        if (t_start < 0) {
            t_start = 0;
        }
        if (t_stop > t_size - 1) {
            t_stop = t_size - 1;
        }
        if (t_start > t_stop || t_start > t_size - 1 || t_stop < 0) {
            return Status::OK();
        } else {
            ZIterator *iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
            if (iter == NULL) {
                return Status::Corruption("zscan error");
            }
            int32_t n = 0;
            for (; n < t_start && iter->Valid(); iter->Next(), n++);

            if (n<t_start) {
                delete iter;
                return Status::Corruption("ziterate error");
            } else {
                for (; n <= t_stop && iter->Valid(); iter->Next(), n++) {
                    db_key = EncodeZSetKey(key, iter->member());
                    score_key = EncodeZScoreKey(key, iter->member(), iter->score());
                    batch.Delete(db_key);
                    batch.Delete(score_key);
                    (*count)++;
                }
                delete iter;
                if (IncrZLen(key, -(*count), batch) == 0) {
                    s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
                    return s;
                } else {
                    return Status::Corruption("incr zsize error");
                }
            }
        }
    } else {
        return Status::Corruption("get zsize error");
    }
}

Status Nemo::ZRemrangebyscore(const std::string &key, const double mn, const double mx, int64_t* count, bool is_lo, bool is_ro) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    rocksdb::WriteBatch batch;
    std::string score_key;
    std::string size_key;
    std::string db_key;
    *count = 0;
    Status s;
    double start = is_lo ? mn + eps : mn;
    double stop = is_ro ? mx - eps : mx;
    RecordLock l(&mutex_zset_record_, key);
    //MutexLock l(&mutex_zset_);
    
    ZIterator *iter = ZScan(key, start, stop, -1);
    for (; iter->Valid(); iter->Next()) {
        db_key = EncodeZSetKey(key, iter->member());
        score_key = EncodeZScoreKey(key, iter->member(), iter->score());
        batch.Delete(db_key);
        batch.Delete(score_key);
        (*count)++;
    }
    delete iter;
    if (IncrZLen(key, -(*count), batch) == 0) {
        s = zset_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return Status::Corruption("incr zsize error");
    }
}

int Nemo::DoZSet(const std::string &key, const double score, const std::string &member, rocksdb::WriteBatch &writebatch) {
    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);

    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    if (s.ok()) {
        double dval = *((double *)old_score.data());
        /* find the same value */ 
        if (fabs(dval - score) < eps) {
            return 0;
        } else {
          score_key = EncodeZScoreKey(key, member, dval);
          writebatch.Delete(score_key);
          score_key = EncodeZScoreKey(key, member, score);
          writebatch.Put(score_key, "");

          std::string buf;
          buf.append((char *)(&score), sizeof(double));
          writebatch.Put(db_key, buf);
          return 1;
        }
    } else if (s.IsNotFound()) {
        score_key = EncodeZScoreKey(key, member, score);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&score), sizeof(double));
        writebatch.Put(db_key, buf);
        return 2;
    } else {
        return -1;
    }
}

Status Nemo::ZDelKey(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;
    *res = 0;
    
    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        s = Status::NotFound("");
      } else {
        *res = 1;
        len = 0;
        //MutexLock l(&mutex_zset_);
        s = zset_db_->PutWithKeyVersion(rocksdb::WriteOptions(), size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
      }
    }

    return s;
}

Status Nemo::ZExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_zset_record_, key);

    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("empty zset");
      }

      if (seconds > 0) {
        //MutexLock l(&mutex_zset_);
        s = zset_db_->PutWithKeyTTL(rocksdb::WriteOptions(), size_key, val, seconds);
      } else { 
        int64_t count;
        s = ZDelKey(key, &count);
      }
      *res = 1;
    }
    return s;
}

Status Nemo::ZTTL(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;
    RecordLock l(&mutex_zset_record_, key);

    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        int32_t ttl;
        s = zset_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        *res = ttl;
    }
    return s;
}

Status Nemo::ZPersist(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_zset_record_, key);
    *res = 0;
    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);

    if (s.ok()) {
        int32_t ttl;
        s = zset_db_->GetKeyTTL(rocksdb::ReadOptions(), size_key, &ttl);
        if (s.ok() && ttl >= 0) {
            //MutexLock l(&mutex_zset_);
            s = zset_db_->Put(rocksdb::WriteOptions(), size_key, val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::ZExpireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    std::string val;

    RecordLock l(&mutex_zset_record_, key);

    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
      int64_t len = *(int64_t *)val.data();
      if (len <= 0) {
        return Status::NotFound("empty zset");
      }

      std::time_t cur = std::time(0);
      if (timestamp <= cur) {
        int64_t count;
        s = ZDelKey(key, &count);
      } else {
        //MutexLock l(&mutex_zset_);
        s = zset_db_->PutWithExpiredTime(rocksdb::WriteOptions(), size_key, val, timestamp);
      }
      *res = 1;
    }
    return s;
}

int Nemo::IncrZLen(const std::string &key, int64_t by, rocksdb::WriteBatch &writebatch) {
    Status s;
    std::string size_key = EncodeZSizeKey(key);
    int64_t size = ZCard(key);
    if (size == -1) {
        return -1;
    }
    size += by;
    writebatch.Put(size_key, rocksdb::Slice((char *)&size, sizeof(int64_t)));

    //writebatch.Put(size_key, std::to_string(size));

 //   if (size <= 0) {
 //       writebatch.Delete(size_key);
 //   } else {
 //       writebatch.Put(size_key, std::to_string(size));
 //   }
    return 0;
}

