#include <ctime>

#include "nemo_list.h"
#include "nemo_mutex.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

//static int32_t ParseMeta(std::string &meta_val, ListMeta &meta) {
bool ListMeta::DecodeFrom(const std::string &meta_val) {
  if (meta_val.size() != sizeof(int64_t) * 4) {
    return false;
  }

  len = *((int64_t *)(meta_val.data()));
  left = *((int64_t *)(meta_val.data() + sizeof(int64_t)));
  right = *((int64_t *)(meta_val.data() + sizeof(int64_t) * 2));
  cur_seq = *((int64_t *)(meta_val.data() + sizeof(int64_t) * 3));
  return true;
}
bool ListMeta::EncodeTo(std::string& meta_val) {
  meta_val.clear();
  meta_val.append((char *)&len, sizeof(int64_t));
  meta_val.append((char *)&left, sizeof(int64_t));
  meta_val.append((char *)&right, sizeof(int64_t));
  meta_val.append((char *)&cur_seq, sizeof(int64_t));
  return true;
}
std::string ListMeta::ToString() {
  char buf[32];
  std::string res("Len : ");
  Int64ToStr(buf, 32, len);
  res.append(buf);
  res.append(", Left : ");
  Int64ToStr(buf, 32, left);
  res.append(buf);
  res.append(", Right : ");
  Int64ToStr(buf, 32, right);
  res.append(buf);
  res.append(", Cur_seq : ");
  res.append(buf);
  return res;
}

Status Nemo::LGetMetaByKey(const std::string& key, ListMeta& meta) {
  std::string meta_val, meta_key = EncodeLMetaKey(key);
  Status s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
  if (!s.ok()) {
    return s;
  }
  meta.DecodeFrom(meta_val);
  return Status::OK();
}
Status Nemo::LChecknRecover(const std::string& key) {
  RecordLock l(&mutex_list_record_, key);
  ListMeta meta;
  Status s = LGetMetaByKey(key, meta);
  if (!s.ok()) {
    return s;
  }
  // Traverse from head and find the break before point
  int count = 0;
  int64_t next = meta.left, cur = 0;
  ListData cur_data;
  rocksdb::WriteBatch batch;
  std::string cur_listkey, en_val, raw_val;
  do {
    cur_listkey = EncodeListKey(key, next);
    s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
    if (s.IsNotFound()) { 
      // We cant find the next one, so we now stand on the break before point
      break;
    } else if (!s.ok()) {
      return Status::Corruption("get listkey error");
    }
    int64_t tmp_next = 0, tmp_priv = 0;
    DecodeListVal(en_val, &tmp_priv, &tmp_next, raw_val);
    if (tmp_priv != cur) {
      // inconsistent
      EncodeListVal(raw_val, cur, tmp_next, en_val);
      batch.Put(cur_listkey, en_val);
    }
    ++count;
    cur_data.reset(cur, tmp_next, raw_val);
    cur = next;
    next = tmp_next;
  } while (next != 0);

  // Success iterator from head to the end
  if (next == 0 && cur == meta.right) {
    return Status::OK();
  }
  
  if (cur == 0) {
    //Delete if no data found
    batch.Delete(EncodeLMetaKey(key)); 
  } else {
    // Truncate list
    std::string right_key = EncodeListKey(key, cur);
    std::string right_val;
    EncodeListVal(cur_data.val, cur_data.priv, 0, right_val);
    batch.Put(right_key, right_val);

    // Change Meta
    meta.len = count;
    meta.right = cur;
    std::string meta_val;
    meta.EncodeTo(meta_val);
    batch.Put(EncodeLMetaKey(key), meta_val);
  }
  return list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
}

int32_t Nemo::L2R(const std::string &key, const int64_t index, const int64_t left, int64_t *priv, int64_t *cur, int64_t *next) {
    int64_t t = index;
    int64_t t_cur = left;
    Status s;
    std::string db_key;
    std::string en_val;
    std::string raw_val;
    while (t >= 0) {
        *cur = t_cur;
        db_key = EncodeListKey(key, *cur);
        s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val); 
        if (!s.ok()) {
            break;
        }
        DecodeListVal(en_val, priv, next, raw_val); 
        t--;
        t_cur = *next;
    }
    if ( t < 0 ) {
        return 0;
    } else {
        return -1;
    }
} 

int32_t Nemo::R2L(const std::string &key, const int64_t index, const int64_t right, int64_t *priv, int64_t *cur, int64_t *next) {
    int64_t t = index;
    int64_t t_cur = right;
    Status s;
    std::string db_key;
    std::string en_val;
    std::string raw_val;
    while (t >= 0) {
        *cur = t_cur;
        db_key = EncodeListKey(key, *cur);
        s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val); 
        if (!s.ok()) {
            break;
        }
        DecodeListVal(en_val, priv, next, raw_val); 
        t--;
        t_cur = *priv;
    }
    if ( t < 0 ) {
        return 0;
    } else {
        return -1;
    }
} 

Status Nemo::LIndex(const std::string &key, const int64_t index, std::string *val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta_val;
    ListMeta meta;
    RecordLock l(&mutex_list_record_, key);
    
    int64_t priv;
    int64_t cur;
    int64_t next;
    std::string en_val;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len <= 0) {
                return Status::NotFound("not found the key");
            }
            if (index >= meta.len || -index > meta.len ) {
                return Status::Corruption("index out of range");
            }
            if (index >= 0) {
                if (L2R(key, index, meta.left, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            } else {
                if (R2L(key, -index-1, meta.right, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            }
            std::string db_key = EncodeListKey(key, cur);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            DecodeListVal(en_val, &priv, &next, *val);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LLen(const std::string &key, int64_t *llen) {
    Status s;
    std::string meta_key = EncodeLMetaKey(key);
    std::string meta_val;
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if(meta_val.size() != sizeof(int64_t) * 4) {
            return Status::Corruption("list meta error");
        }
        *llen = *((int64_t *)(meta_val.data()));

        if (*llen <= 0) {
            return Status::NotFound("not found the key");
        }
    } else {
        *llen = 0;
    }
    return s;
}

Status Nemo::LPush(const std::string &key, const std::string &val, int64_t *llen) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_val;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    //sleep(8);

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.left > 0) {
                std::string l_key = EncodeListKey(key, meta.left);
                s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get left error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, meta.cur_seq, next, en_val);
                batch.Put(l_key, en_val);
            }

            std::string db_key = EncodeListKey(key, meta.cur_seq);
            EncodeListVal(val, 0, meta.left, en_val);
            batch.Put(db_key, en_val);

            meta.len++;
            meta.left = meta.cur_seq;
            if (meta.right == 0) {
                meta.right = meta.cur_seq;
            }
            meta.cur_seq++;
            //char buf[64];
            //memcpy(buf, (char *)&meta, sizeof(ListMeta));

            //batch.Put(meta_key, std::string((char *)&meta, sizeof(ListMeta)));
            meta.EncodeTo(meta_val);
            batch.Put(meta_key, meta_val);
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            *llen = meta.len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        ListMeta meta(1, 1, 1, 2); // | len | left | right | cur_seq |
        
        meta_val.reserve(4 * sizeof(int64_t));
        meta.EncodeTo(meta_val);

        batch.Put(meta_key, meta_val);
        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(key, 1), en_val);
        s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        *llen = 1;
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LPop(const std::string &key, std::string *val) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_val;

    int64_t priv = 0;
    int64_t next = 0;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len <= 0) {
                return Status::NotFound("not found key");
            }

            std::string db_key = EncodeListKey(key, meta.left);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get meta.left error");
            }
            DecodeListVal(en_val, &priv, &next, *val);

            meta.left = next;
            if (next != 0) {
                std::string l_key = EncodeListKey(key, next);
                s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next left error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, 0, next, en_val);
                batch.Put(l_key, en_val);
            }

            --meta.len;
            if (meta.len == 0) {
                meta.right = 0;
                meta.cur_seq = 1;
            }
            //memcpy(meta_val, (char *)&meta, sizeof(ListMeta));
            //batch.Put(meta_key, meta_val);
            meta.EncodeTo(meta_val);
            batch.Put(meta_key, meta_val);

            batch.Delete(db_key);
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LPushx(const std::string &key, const std::string &val, int64_t *llen) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    ListMeta meta;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len == 0) {
                *llen = 0;
                return Status::NotFound("not found the key");
            } else if (meta.len > 0) {
                s = LPush(key, val, llen);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LRange(const std::string &key, const int64_t begin, const int64_t end, std::vector<IV> &ivs) {
    Status s;
    ListMeta meta;
    std::string meta_val;
    std::string res;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
//    MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);

    const rocksdb::Snapshot* ss = list_db_->GetSnapshot();
    rocksdb::ReadOptions rop = rocksdb::ReadOptions();
    rop.snapshot = ss;
    s = list_db_->Get(rop, meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len == 0) {
                return Status::NotFound("not found the key");
            } else if (meta.len > 0) {
                int64_t index_b = begin >= 0 ? begin : meta.len + begin;
                int64_t index_e = end >= 0 ? end : meta.len + end;
                if (index_b > index_e || index_b >= meta.len || index_e < 0) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::OK();
                }
                if (index_b < 0) {
                    index_b = 0;
                }
                if (index_e >= meta.len) {
                    index_e = meta.len - 1;
                }
                int64_t priv;
                int64_t cur;
                int64_t next;
                if (L2R(key, index_b, meta.left, &priv, &cur, &next) != 0) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::Corruption("error in iterate");
                }
                int32_t t = index_e - index_b + 1;
                std::string i_key;
                std::string i_en_val;
                std::string i_raw_val;
                int32_t i = 0;
                while (i<t) {
                    i_key = EncodeListKey(key, cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), i_key, &i_en_val);
                    if (!s.ok()) {
                        break;
                    }
                    DecodeListVal(i_en_val, &priv, &next, i_raw_val);
                    ivs.push_back(IV{i+index_b, i_raw_val});
                    cur = next;
                    i++;
                }
                if (i<t) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::Corruption("get element error");
                }
                list_db_->ReleaseSnapshot(ss);
                return Status::OK();
            } else {
                list_db_->ReleaseSnapshot(ss);
                return Status::Corruption("get invalid listlen");
            }
        } else {
            list_db_->ReleaseSnapshot(ss);
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        list_db_->ReleaseSnapshot(ss);
        return Status::NotFound("not found the key");
    } else {
        list_db_->ReleaseSnapshot(ss);
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LSet(const std::string &key, const int64_t index, const std::string &val) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    std::string meta_val;
    ListMeta meta;

    int64_t priv;
    int64_t cur;
    int64_t next;
    std::string en_val;
    std::string meta_key = EncodeLMetaKey(key);
    RecordLock l(&mutex_list_record_, key);
    //MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len <= 0) {
                return Status::NotFound("not found key");
            }
            if ( index >= meta.len || -index > meta.len ) {
                return Status::Corruption("index out of range");
            }
            if (index >= 0) {
                if (L2R(key, index, meta.left, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            } else {
                if (R2L(key, -index-1, meta.right, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            }
            std::string db_key = EncodeListKey(key, cur);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            std::string raw_val;
            DecodeListVal(en_val, &priv, &next, raw_val);
            EncodeListVal(val, priv, next, en_val);
            s = list_db_->Put(rocksdb::WriteOptions(), db_key, en_val);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LTrim(const std::string &key, const int64_t begin, const int64_t end) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    ListMeta meta;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
    rocksdb::WriteBatch batch;
    RecordLock l(&mutex_list_record_, key);
    //MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len == 0) {
                return Status::NotFound("not found the key");
            } else if (meta.len > 0) {
                int64_t index_b = begin >= 0 ? begin : meta.len + begin;
                int64_t index_e = end >= 0 ? end : meta.len + end;
                if (index_b > index_e || index_b >= meta.len || index_e < 0) {
                    index_b = meta.len;
                    index_e = meta.len;
                }
                if (index_b < 0) {
                    index_b = 0;
                }
                if (index_e >= meta.len) {
                    index_e = meta.len - 1;
                }
                int64_t trim_num = 0;
                int64_t t_cur;
                int64_t priv = 0;
                int64_t next = 0;
                std::string en_val;
                std::string raw_val;
                t_cur = meta.left;
                for (int64_t i = 0; i < index_b; i++) {
                    db_key = EncodeListKey(key, t_cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    batch.Delete(db_key);
                    t_cur = next;
                    trim_num++;
                }
                if (next != 0) {
                    meta.left = next;
                    //*((int64_t *)(meta_val.data() + sizeof(int64_t))) = next;
                    std::string l_key = EncodeListKey(key, next);
                    s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get next meta.left error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, 0, next, en_val);
                    s = list_db_->Put(rocksdb::WriteOptions(), l_key, en_val);
                    //batch.Put(l_key, en_val);
                }

                priv = 0;
                t_cur = meta.right;
                for (int64_t i = meta.len - 1; i > index_e; i--) {
                    db_key = EncodeListKey(key, t_cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    batch.Delete(db_key);
                    t_cur = priv;
                    trim_num++;
                }
                if (priv != 0) {
                    meta.right = priv;
                    //*((int64_t *)(meta_val.data() + sizeof(int64_t) * 2)) = priv;
                    std::string r_key = EncodeListKey(key, priv);
                    s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get next meta.right error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, priv, 0, en_val);
                    s = list_db_->Put(rocksdb::WriteOptions(), r_key, en_val);
                    //batch.Put(r_key, en_val);
                }
                if (trim_num < meta.len) {
                    meta.len -= trim_num;
                } else {
                   meta = ListMeta(); 
                }

                //memcpy(meta_val, (char *)&meta, sizeof(ListMeta));
                //batch.Put(meta_key, std::string((char *)&meta, sizeof(ListMeta)));

                meta.EncodeTo(meta_val);
                batch.Put(meta_key, meta_val);
                s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPush(const std::string &key, const std::string &val, int64_t *llen) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_val;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.right != 0) {
                std::string r_key = EncodeListKey(key, meta.right);
                s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get meta.right error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, priv, meta.cur_seq, en_val);
                batch.Put(r_key, en_val);
            }

            std::string db_key = EncodeListKey(key, meta.cur_seq);
            EncodeListVal(val, meta.right, 0, en_val);
            batch.Put(db_key, en_val);

            meta.len++;
            if (meta.left == 0) {
              meta.left = meta.cur_seq;
            }
            meta.right = meta.cur_seq;
            ++meta.cur_seq;
            //memcpy(meta_val, (char *)&meta, sizeof(ListMeta));
            //batch.Put(meta_key, meta_val);

            //batch.Put(meta_key, std::string((char *)&meta, sizeof(ListMeta)));

            meta.EncodeTo(meta_val);
            batch.Put(meta_key, meta_val);
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            *llen = meta.len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        ListMeta meta(1, 1, 1, 2);
        
        //std::string meta_val((char *)&meta, 4 * sizeof(int64_t));
        
        meta.EncodeTo(meta_val);
        batch.Put(meta_key, meta_val);
        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(key, 1), en_val);
        s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        *llen = 1;
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPop(const std::string &key, std::string *val) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_val;

    int64_t priv = 0;
    int64_t next = 0;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len <= 0) {
                return Status::NotFound("not found key");
            }

            std::string db_key = EncodeListKey(key, meta.right);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get meta.right error");
            }
            DecodeListVal(en_val, &priv, &next, *val);

            meta.right = priv;

            if (priv != 0) {
                std::string r_key = EncodeListKey(key, priv);
                s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next meta.right error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, priv, 0, en_val);
                batch.Put(r_key, en_val);
            }
            
            --meta.len;
            if (meta.len == 0) {
                meta.left = 0;
                meta.cur_seq = 1;
            }
            //memcpy(meta_val, (char *)&meta, sizeof(ListMeta));
            //batch.Put(meta_key, meta_val);

            //batch.Put(meta_key, std::string((char *)&meta, sizeof(ListMeta)));

            meta.EncodeTo(meta_val);
            batch.Put(meta_key, meta_val);

            batch.Delete(db_key);
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPushx(const std::string &key, const std::string &val, int64_t *llen) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    
    Status s;
    ListMeta meta;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len == 0) {
                *llen = 0;
                return Status::NotFound("not found the key");
            } else if (meta.len > 0) {
                s = RPush(key, val, llen);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}


Status Nemo::RPopLPushInternal(const std::string &src, const std::string &dest, std::string &val) {
    if (src.size() >= KEY_MAX_LENGTH || src.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_r;
    std::string meta_l;
    std::string db_key_r;
    std::string meta_key_r = EncodeLMetaKey(src);
    std::string db_key_l;
    std::string meta_key_l = EncodeLMetaKey(dest);

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key_r, &meta_r);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_r)) {
            if (meta.len == 0 || meta.right == 0) {
                return Status::NotFound("not found the source key");
            }
            std::string en_val;
            std::string raw_val;
            int64_t priv = 0;
            int64_t next = 0;
            db_key_r = EncodeListKey(src, meta.right);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key_r, &en_val);
            DecodeListVal(en_val, &priv, &next, val);
            meta.right = priv;
            if (priv != 0) {
                std::string r_key = EncodeListKey(src, priv);
                s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next meta.right error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, priv, 0, en_val);
                batch.Put(r_key, en_val);
            }
            --meta.len;
            if (meta.len == 0) { // change meta.left to 0 when no element
                meta.left = 0;
            }
            //memcpy(meta_r, (char *)&meta, sizeof(ListMeta));
            //s = list_db_->Put(rocksdb::WriteOptions(), meta_key_r, meta_r);

            std::string meta_val;
            meta.EncodeTo(meta_val);
            s = list_db_->Put(rocksdb::WriteOptions(), meta_key_r, meta_val);
            
            batch.Delete(db_key_r);
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the source key");
    } else {
        return Status::Corruption("get listmeta error");
    }

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key_l, &meta_l);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_l)) {
            std::string en_val;

            if (meta.left > 0) {
                db_key_l = EncodeListKey(dest, meta.left);
                std::string raw_val;
                int64_t priv = 0;
                int64_t next = 0;
                s = list_db_->Get(rocksdb::ReadOptions(), db_key_l, &en_val);
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, meta.cur_seq, next, en_val);
                batch.Put(db_key_l, en_val);
            }

            db_key_l = EncodeListKey(dest, meta.cur_seq);
            EncodeListVal(val, 0, meta.left, en_val);
            batch.Put(db_key_l, en_val);
            meta.len++;
            meta.left = meta.cur_seq;
            if (meta.right == 0) {
              meta.right = meta.cur_seq;
            }
            ++meta.cur_seq;
            //memcpy(meta_l, (char *)&meta, sizeof(ListMeta));
            //batch.Put(meta_key_l, meta_l);

            std::string meta_val;
            meta.EncodeTo(meta_val);
            batch.Put(meta_key_l, meta_val); 
            s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        ListMeta meta(1, 1, 1, 2);
        std::string en_val;
        
        //std::string meta_str((char *)meta, 4 * sizeof(int64_t));
        //batch.Put(meta_key_l, meta_str);
        //batch.Put(meta_key_l, std::string((char *)&meta, sizeof(ListMeta)));

        std::string meta_val;
        meta.EncodeTo(meta_val);
        batch.Put(meta_key_l, meta_val);

        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(dest, 1), en_val);
        s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}
Status Nemo::RPopLPush(const std::string &src, const std::string &dest, std::string &val) {
    RecordLock l1(&mutex_list_record_, dest);
    if (src == dest) {
      return RPopLPushInternal(src, dest, val);
    } else {
      RecordLock l2(&mutex_list_record_, src);
      return RPopLPushInternal(src, dest, val);
    }
}

Status Nemo::LInsert(const std::string &key, Position pos, const std::string &pivot, const std::string &val, int64_t *llen) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    ListMeta meta;
    std::string meta_val;
    int64_t before_seq;
    int64_t after_seq;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {

            // traverse to find pivot
            next = meta.left;
            int64_t tmp_seq;
            do {
              tmp_seq = next;
              std::string cur_listkey = EncodeListKey(key, tmp_seq);
              s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
              if (!s.ok()) {
                return Status::Corruption("get listkey error");
              }
              DecodeListVal(en_val, &priv, &next, raw_val);
            } while (next != 0 && raw_val != pivot);

            if (raw_val == pivot) {
                if (pos == AFTER) {
                    before_seq = tmp_seq;
                    after_seq = next;
                } else {
                    before_seq = priv;
                    after_seq = tmp_seq;
                }

                // modify before node
                if (before_seq != 0) {
                    std::string cur_listkey = EncodeListKey(key, before_seq);
                    s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, priv, meta.cur_seq, en_val);
                    batch.Put(cur_listkey, en_val);
                }
                // modify after node
                if (after_seq != 0) {
                    std::string cur_listkey = EncodeListKey(key, after_seq);
                    s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, meta.cur_seq, next, en_val);
                    batch.Put(cur_listkey, en_val);
                }
                // add new node
                std::string add_key = EncodeListKey(key, meta.cur_seq);
                EncodeListVal(val, before_seq, after_seq, en_val);
                batch.Put(add_key, en_val);

                meta.len++;
                if (before_seq == 0) {
                    meta.left = meta.cur_seq;
                }
                if (after_seq == 0) {
                    meta.right = meta.cur_seq;
                }
                ++meta.cur_seq;
                //memcpy(meta_val, (char *)&meta, sizeof(ListMeta));
                //batch.Put(meta_key, meta_val);
                
                //batch.Put(meta_key, std::string((char *)&meta, sizeof(ListMeta)));

                meta.EncodeTo(meta_val);
                batch.Put(meta_key, meta_val);
                s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
                *llen = meta.len;
                return s;
            } else {
                *llen = -1;
                return s;
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return s;
    } else {
        *llen = 0;
        return Status::Corruption("get key error");
    }
}

Status Nemo::LRem(const std::string &key, const int64_t count, const std::string &val, int64_t *rem_count) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    Status s;
    rocksdb::WriteBatch batch;
    std::string meta_val;
    ListMeta meta;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    //MutexLock l(&mutex_list_);
    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
        if (meta.DecodeFrom(meta_val)) {
            if (meta.len == 0) {
                *rem_count = 0;
                return Status::NotFound("not found key");
            }

            int64_t tmp_seq;
            int64_t tmp_priv, tmp_next;
            priv = meta.right;
            next = meta.left;
            *rem_count = 0;

            int64_t total_rem = abs(count);
            if (total_rem > meta.len) {
                total_rem = meta.len;
            }

            do {
              if (meta.len == 0) return s;

              if (count < 0) {
                  tmp_seq = priv;
              } else {
                  tmp_seq = next;
              }

              std::string cur_listkey = EncodeListKey(key, tmp_seq);
              s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
              if (!s.ok()) {
                return Status::Corruption("get listkey error");
              }
              DecodeListVal(en_val, &priv, &next, raw_val);

              if (raw_val == val) {
                (*rem_count)++;

                // modify before node
                if (priv != 0) {
                    std::string priv_listkey = EncodeListKey(key, priv);
                    s = list_db_->Get(rocksdb::ReadOptions(), priv_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &tmp_priv, &tmp_next, raw_val);
                    EncodeListVal(raw_val, tmp_priv, next, en_val);
                    batch.Put(priv_listkey, en_val);
                }
                // modify after node
                if (next != 0) {
                    std::string next_listkey = EncodeListKey(key, next);
                    s = list_db_->Get(rocksdb::ReadOptions(), next_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &tmp_priv, &tmp_next, raw_val);
                    EncodeListVal(raw_val, priv, tmp_next, en_val);
                    batch.Put(next_listkey, en_val);
                }
                // delete new node
                batch.Delete(cur_listkey);

                meta.len--;
                if (meta.len == 0) {
                  batch.Delete(meta_key);
                } else {
                  if (priv == 0) {
                    meta.left = next;
                  }
                  if (next == 0) {
                    meta.right = priv;
                  }
                  //memcpy(meta_val, (char *)&meta, sizeo(ListMeta));
                  //batch.Put(meta_key, meta_val);

                  meta.EncodeTo(meta_val);
                  batch.Put(meta_key, meta_val);
                }
                s = list_db_->WriteWithOldKeyTTL(rocksdb::WriteOptions(), &batch);
              }
            } while ((count == 0 || *rem_count < total_rem) 
                     && ((count < 0 && priv != 0) || (count >= 0 && next != 0)));
            return Status::OK();
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *rem_count = 0;
        return s;
    } else {
        return Status::Corruption("get key error");
    }
}

Status Nemo::LDelKey(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    ListMeta meta;
    Status s;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);

    *res = 0;

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.ok()) {
      if (!meta.DecodeFrom(meta_val) || meta.len == 0) {
        s = Status::NotFound("not found key");
      } else {
        meta = ListMeta();

        *res = 1;
        std::string new_val;
        meta.EncodeTo(new_val);
        s = list_db_->PutWithKeyVersion(rocksdb::WriteOptions(), meta_key, new_val);
      }
    }

    return s;
}

Status Nemo::LExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    
    ListMeta meta;
    Status s;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);

    RecordLock l(&mutex_list_record_, key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        if (!meta.DecodeFrom(meta_val)) {
            return Status::NotFound("not found key");
        }
        if (meta.len == 0) {
            return Status::NotFound("not found key");
        }

        if (seconds > 0) {
            //MutexLock l(&mutex_list_);
            s = list_db_->PutWithKeyTTL(rocksdb::WriteOptions(), meta_key, meta_val, seconds);
        } else { 
            int64_t count; 
            s = LDelKey(key, &count);
        }
    }

    return s;
}

Status Nemo::LTTL(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    ListMeta meta;
    Status s;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);

    RecordLock l(&mutex_list_record_, key);

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        if (!meta.DecodeFrom(meta_val)) {
            return Status::NotFound("not found key");
        }
        if (meta.len <= 0) {
            return Status::NotFound("not found key");
        }
        int32_t ttl;
        s = list_db_->GetKeyTTL(rocksdb::ReadOptions(), meta_key, &ttl);
        *res = ttl;
    }

    return s;
}

Status Nemo::LPersist(const std::string &key, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }

    ListMeta meta;
    Status s;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);

    RecordLock l(&mutex_list_record_, key);

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);

    if (s.ok()) {
        if (!meta.DecodeFrom(meta_val)) {
            return Status::NotFound("not found key");
        }
        if (meta.len <= 0) {
            return Status::NotFound("not found key");
        }

        int32_t ttl;
        s = list_db_->GetKeyTTL(rocksdb::ReadOptions(), meta_key, &ttl);
        if (s.ok() && ttl >= 0) {
            //MutexLock l(&mutex_list_);
            s = list_db_->Put(rocksdb::WriteOptions(), meta_key, meta_val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::LExpireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    if (key.size() >= KEY_MAX_LENGTH || key.size() <= 0) {
       return Status::InvalidArgument("Invalid key length");
    }
    
    ListMeta meta;
    Status s;
    std::string meta_val;
    std::string meta_key = EncodeLMetaKey(key);

    RecordLock l(&mutex_list_record_, key);

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta_val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        if (!meta.DecodeFrom(meta_val)) {
            return Status::NotFound("not found key");
        }
        if (meta.len == 0) {
            return Status::NotFound("not found key");
        }

      std::time_t cur = std::time(0);
      if (timestamp <= cur) { 
        int64_t count;
        s = LDelKey(key, &count);
      } else { 
        //MutexLock l(&mutex_list_);
        s = list_db_->PutWithExpiredTime(rocksdb::WriteOptions(), meta_key, meta_val, timestamp);
      }
    }

    return s;
}
