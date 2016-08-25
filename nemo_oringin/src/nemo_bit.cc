#include <ctime>
#include <climits>
#include <list>
#include <climits>

#include "nemo.h"
#include "nemo_mutex.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::BitSet(const std::string &key, const std::int64_t offset, const int64_t on, int64_t* res) {
    std::string value;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &value);
    if (s.ok() || s.IsNotFound()) {
        size_t byte = offset >> 3;
        size_t bit = 7 - (offset & 0x7);
        char byte_val;
        size_t value_lenth = value.length();
        if (byte > value_lenth) {
            *res = 0;
            byte_val = 0;
        } else {
            *res = (value[byte] & 1<<bit) >> bit;
            byte_val = value[byte];
        }
        byte_val &= (char) ~(1 << bit);
        byte_val |= (char) ((on & 0x1) << bit);
        if (byte <= value_lenth) {
            value.replace(byte, 1, &byte_val, 1);
        } else {
            value.append(byte - value_lenth -1, 0);
            value.append(1, byte_val);
        }
        s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        if (s.ok()) {
            return Status::OK();
        } else {
            return Status::Corruption("bitset: set failed");
        }
    } else {
        return Status::Corruption("bitset: get failed");
    }
    return Status::OK();
}

Status Nemo::BitGet(const std::string &key, const std::int64_t offset, std::int64_t* res) {
    std::string value;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &value);
    if (s.ok() || s.IsNotFound()) {
        size_t byte = offset >> 3;
        size_t bit = 7 - (offset & 0x7);
        if (byte > value.length()) {
            *res = 0;
        } else {
            *res = (value[byte] & 1<<bit) >> bit;
        }
    } else {
      return s;
    }
    return Status::OK();
}

int GetBitCount(const unsigned char* value,int64_t bytes){
  int bit_num = 0;
  static const unsigned char bitsinbyte[256] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};
  for (int i = 0; i < bytes; i++) {
    bit_num += bitsinbyte[(unsigned int)value[i]];
  }
  return bit_num;
}

Status Nemo::BitCount(const std::string &key, std::int64_t* res) {
    std::string value_str;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &value_str);
    if (s.ok()) {
        const unsigned char * value = (const unsigned char *) (value_str.data());
        int64_t value_length = value_str.length();
        int64_t start_offset = 0;
        int64_t end_offset = std::max(value_length - 1, (int64_t)0);
        *res = GetBitCount(value + start_offset, end_offset - start_offset + 1);
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return s;
    }

    return Status::OK();
}

Status Nemo::BitCount(const std::string &key, std::int64_t start_offset, std::int64_t end_offset, std::int64_t* res) {
    std::string value_str;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &value_str);
    if (s.ok()) {
        const unsigned char * value = (const unsigned char *) (value_str.data());
        int value_length = value_str.length();
        if (start_offset < 0) {
            start_offset = start_offset + value_length;
        }
        if (end_offset < 0) {
            end_offset = end_offset + value_length;
        }
        if (start_offset < 0) {
            start_offset = 0;
        }
        if (end_offset < 0) {
            end_offset = 0;
        }
        // start_offset_ bigger than length returns 0
        if (start_offset > value_length) {
            start_offset = value_length;
        }
        // end_offset_ bigger than length returns bits from start_offset to end of the string
        if (end_offset >= value_length) {
            end_offset = value_length - 1;
        }
        if (start_offset > end_offset) {
            *res = 0;
            return Status::OK();
        }
        *res = GetBitCount(value + start_offset, end_offset - start_offset + 1);
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return s;
    }
    return Status::OK();
}



// When can't find bit val from s[0] to s[bytes-1]:
//  return 8*bytes if bit = 0
//  return -1 if bit = 1
int GetBitPos(const unsigned char *s, unsigned int bytes, int bit) {
    uint64_t word = 0;
    uint64_t skip_val = 0;
    uint64_t * l = (uint64_t *) s;
    int pos = 0;
    if (bit == 0) {
      skip_val = std::numeric_limits<uint64_t>::max();
    } else {
      skip_val = 0;
    }
    // skip 8 bytes at one time, find the first int64 that should not be skipped
    while (bytes >= sizeof(*l)) {
      if (*l != skip_val) {
        break;
      }
      l++;
      bytes = bytes - sizeof(*l);
      pos = pos + 8 * sizeof(*l);
    }
    unsigned char * c = (unsigned char *) l;
    for (unsigned int j = 0; j < sizeof(*l); j++) {
      word = word << 8;
      if (bytes) {
        word = word | *c;
        c++;
        bytes --;
      }
    }
    if (bit == 1 && word == 0) {
      return -1;
    }
    // set each bit of mask to 0 except msb
    uint64_t mask = std::numeric_limits<uint64_t>::max();
    mask = mask >> 1;
    mask = ~(mask);
    while (mask) {
      if (((word & mask) != 0) == bit) {
        return pos;
      }
      pos++;
      mask = mask >> 1;
    }
    return pos;

}

// [bitpos key 1 ] returns -1 if there is no 1 in the value we found
// [bitpos key 0 ] returns 8*value_length if there is no 0 in the value we found
Status Nemo::BitPos(const std::string &key, const int64_t bit_val, std::int64_t* res) {
    Status s;
    std::string value_str;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &value_str);
    if (s.ok()) {
        const unsigned char * value = (const unsigned char *) (value_str.data());
        int value_length = value_str.length();
        int64_t start_offset = 0;
        int64_t end_offset = std::max(value_length - 1, 0);
        int bytes = end_offset - start_offset + 1;
        int pos = GetBitPos(value + start_offset, bytes, bit_val);
        if (pos != -1) {
            pos = pos + 8 * start_offset;
        }
        *res = pos;
    } else if (s.IsNotFound()) {
        if (bit_val == 1)
            *res = -1;
        else if (bit_val == 0) {
            *res = 0;
        }
    } else {
        return s;
    }
    return Status::OK();
}

// [bitpos key 0 start  ] returns 8*value_length if there is no 0 in the value we found
// [bitpos key 1 start  ] returns -1 if there is no 1 in the value we found
Status Nemo::BitPos(const std::string &key, const int64_t bit_val, std::int64_t start_offset, std::int64_t* res) {
    Status s;
    std::string value_str;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &value_str);
    if (s.ok()) {
        const unsigned char * value = (const unsigned char *) (value_str.data());
        int value_length = value_str.length();
        int64_t end_offset = std::max(value_length - 1, 0);
        if (start_offset < 0) {
            start_offset = start_offset + value_length;
        }
        if (start_offset < 0) {
            start_offset = 0;
        }
        if (start_offset > end_offset) {
            *res = -1;
            return Status::OK();
        }
        if (start_offset > value_length - 1) {
            *res = -1;
            return Status::OK();
        }
        int bytes = end_offset - start_offset + 1;
        int pos = GetBitPos(value + start_offset, bytes, bit_val);
        if (pos != -1) {
            pos = pos + 8 * start_offset;
        }
        *res = pos;

    } else if (s.IsNotFound()) {
        if (bit_val == 1)
            *res = -1;
        else if (bit_val == 0) {
            *res = 0;
        }
    } else {
        return s;
    }
    return Status::OK();
}

Status Nemo::BitPos(const std::string &key, const int64_t bit_val, std::int64_t start_offset, std::int64_t end_offset, std::int64_t* res) {
    Status s;
    std::string value_str;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &value_str);
    if (s.ok()) {
        const unsigned char * value = (const unsigned char *) (value_str.data());
        int value_length = value_str.length();
        if (start_offset < 0) {
            start_offset = start_offset + value_length;
        }
        if (start_offset < 0) {
            start_offset = 0;
        }
        if (end_offset < 0) {
            end_offset = end_offset + value_length;
        }
        // converting to int64_t just avoid warning 
        if (end_offset > (int64_t)value_str.length() - 1) {
            end_offset = value_length - 1;
        }
        if (end_offset < 0) {
            end_offset = 0;
        }
        if (start_offset > end_offset) {
            *res = -1;
            return Status::OK();
        }
        if (start_offset > value_length - 1) {
            *res = -1;
            return Status::OK();
        }

        int bytes = end_offset - start_offset + 1;
        int pos = GetBitPos(value + start_offset, bytes, bit_val);
        if (pos == (8 * bytes) && bit_val == 0)
            pos = -1;
        if (pos != -1) {
            pos = pos + 8 * start_offset;
        }
        *res = pos;
    } else if (s.IsNotFound()) {
        if (bit_val == 1) {
            *res = -1;
        }
        else if (bit_val == 0) {
            *res = 0;
        }
    } else {
        return s;
    }
    return Status::OK();
}


std::tuple<int64_t, int64_t> Nemo::BitOpGetSrcValue(const std::vector<std::string> &src_keys, std::vector<std::string> &src_values){
    int64_t max_len = 0;
    int64_t min_len = 0;
    int64_t value_len = 0;
    int64_t src_key_num = src_keys.size();
    Status s;
    std::string value_str;
    for (int i = 0; i <= src_key_num - 1; i++) {
        s = kv_db_->Get(rocksdb::ReadOptions(), src_keys[i], &value_str);
        if (s.ok()) {
            src_values.push_back(value_str);
            value_len = value_str.size();
        } else if (s.IsNotFound()) {
            src_values.push_back(std::string(""));
            value_len = 0;
        }
        max_len = std::max(max_len, value_len);
        if (i == 0) {
            min_len = value_len;
        } else {
            min_len = std::min(min_len, value_len);
        }
    }
    return std::make_tuple(max_len, min_len);
}

std::string Nemo::BitOpOperate(BitOpType op, const std::vector<std::string> &src_values, int64_t max_len, int64_t min_len) {
    unsigned char dest_value[max_len];
    std::vector<int64_t> values_length;
    for (uint64_t i = 0; i <= src_values.size() - 1; i++) {
      values_length.push_back(src_values[i].size());
    }
    unsigned char byte;
    unsigned char output;
    for (int64_t j = 0; j <= max_len - 1; j++) {
        if (j <= values_length[0] - 1) {
            output = src_values[0][j];
        } else {
            output = 0;
        }
        if (op == kBitOpNot) {
            output = ~(output);
        }
        for (uint64_t i = 1; i <= src_values.size() - 1; i++) {
            if (values_length[i] - 1 >= j) {
                byte = src_values[i][j];
                log_info("set byte:%c",byte);
            }
            else {
                byte = 0;
                log_info("default byte:%c",byte);
            }
            log_info("output:%c",output);
            switch (op) {
              case kBitOpNot:
                break;
              case kBitOpAnd:
                output &= byte;
                break;
              case kBitOpOr:
                output |= byte;
                break;
              case kBitOpXor:
                output ^= byte;
                break;
              case kBitOpDefault:
                break;
            }
        }
        dest_value[j] = output;
    
    }
    std::string dest_str = std::string(reinterpret_cast<char*>(dest_value),max_len);
    //dest_str.insert(reinterpret_cast<char*>(dest_value), max_len);

    return dest_str;
}

Status Nemo::BitOp(BitOpType op, const std::string &dest_key, const std::vector<std::string> &src_keys, int64_t* result_length) {
    Status s;
    uint64_t src_key_num = src_keys.size();
    if (op == kBitOpNot && src_key_num != 1) {
        return Status::Corruption("bitop src keys number not right");
    } else if (src_key_num < 1) {
        return Status::Corruption("bitop src keys number not right");
    }
    log_info("op:%d",op);
    std::vector<std::string> src_values;
    int64_t max_len;
    int64_t min_len;
    std::tie(max_len, min_len) = BitOpGetSrcValue(src_keys, src_values);
    log_info("max_len:%d min_len:%d",max_len,min_len);
    log_info("src_values num:%d ",src_values.size());
    if (src_values.size() != src_key_num ) {
      return Status::Corruption("bitop not:error getting src values");
    }
    std::string dest_value = BitOpOperate(op, src_values, max_len, min_len);
    *result_length = dest_value.size();
    s = kv_db_->Put(rocksdb::WriteOptions(), dest_key, dest_value);
    if(s.ok()) {
        return Status::OK();
    } else {
        return s;
    }
}


