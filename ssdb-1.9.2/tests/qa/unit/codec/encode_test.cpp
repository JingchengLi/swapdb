#include "gtest/gtest.h"
#include "ssdb/const.h"
#include "codec/encode.h"
#include "util/bytes.h"
#include "ssdb_test.h"
using namespace std;

class EncodeTest : public SSDBTest
{
};

inline uint64_t encodeScore(const double score) {
    int64_t iscore;
    if (score < 0) {
        iscore = (int64_t)(score * 100000LL - 0.5) + ZSET_SCORE_SHIFT;
    } else {
        iscore = (int64_t)(score * 100000LL + 0.5) + ZSET_SCORE_SHIFT;
    }
    return (uint64_t)(iscore);
}

void compare_encode_meta_key(const string & key, char* expectStr){
    string meta_key = encode_meta_key(Bytes(key.data(), key.size()));
    uint16_t slot = (uint16_t)keyHashSlot(key.data(), (int)key.size());
    uint8_t* pslot = (uint8_t*)&slot;
    expectStr[0] = 'M';
    expectStr[1] = pslot[1];
    expectStr[2] = pslot[0];
    memcpy(expectStr+3, key.data(), key.size());
    EXPECT_EQ(0, meta_key.compare(0, key.size()+3, expectStr, key.size()+3));
}

void compare_encode_key_internal(string(*func)(const Bytes & , const Bytes & , uint16_t),const string & key, const string& field, uint16_t version, char* expectStr){
    string key_internal = (*func)(Bytes(key.data(), key.size()), Bytes(field.data(), field.size()), version);
    expectStr[0] = 'S';
    uint16_t keylen = key.size(); 
    uint8_t* pkeylen = (uint8_t*)&keylen;
    expectStr[1] = pkeylen[1];
    expectStr[2] = pkeylen[0];
    memcpy(expectStr+3, key.data(), keylen);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[keylen+3] = pversion[1];
    expectStr[keylen+4] = pversion[0];
    memcpy(expectStr+5+key.size(), field.data(), field.size());
    EXPECT_EQ(0, key_internal.compare(0, 5+keylen+field.size(), expectStr, 5+keylen+field.size()));
}

TEST_F(EncodeTest, Test_encode_meta_key) {
    char* space = new char[maxKeyLen_+3];

    //Some special keys
    uint16_t keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_meta_key(Keys[n], space);

    //Some random keys
    keysNum = 500;
    for(int n = 0; n < keysNum; n++)
    {
        string key = GetRandomKey_(); 
        compare_encode_meta_key(key, space);
    }

    //MaxLength key
    compare_encode_meta_key(GetRandomBytes_(maxKeyLen_), space);

    delete space;
}

TEST_F(EncodeTest, Test_encode_hash_key) {
    char* space = new char[maxFieldLen_+maxKeyLen_+5];
    string key, field;
    uint16_t version;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        version = GetRandomVer_();
        compare_encode_key_internal(encode_hash_key, key, field, version, space);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_key_internal(encode_hash_key, Keys[n], field, version, space);

    //MaxLength key
    compare_encode_key_internal(encode_hash_key, GetRandomBytes_(maxKeyLen_), field, version, space);

    delete space;
}

TEST_F(EncodeTest, Test_encode_set_key) {
    char* space = new char[maxFieldLen_+maxKeyLen_+5];
    string key, field;
    uint16_t version;

    // Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        version = GetRandomVer_();
        compare_encode_key_internal(encode_set_key, key, field, version, space);
    }

    // Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_key_internal(encode_set_key, Keys[n], field, version, space);

    // MaxLength key
    compare_encode_key_internal(encode_set_key, GetRandomBytes_(maxKeyLen_), field, version, space);

    delete space;
} 

TEST_F(EncodeTest, Test_encode_zset_key) {
    char* space = new char[maxFieldLen_+maxKeyLen_+5];
    string key, field;
    uint16_t version;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        version = GetRandomVer_();
        compare_encode_key_internal(encode_zset_key, key, field, version, space);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_key_internal(encode_zset_key, Keys[n], field, version, space);

    //MaxLength key
    compare_encode_key_internal(encode_zset_key, GetRandomBytes_(maxKeyLen_), field, version, space);

    delete space;
}

void compare_encode_zscore_key(const string & key, const string& member, double score, uint16_t version, char* expectStr){
    string key_zscore = encode_zscore_key(key, member, score, version);
    expectStr[0] = 'z';
    uint16_t keylen = key.size(); 
    uint8_t* pkeylen = (uint8_t*)&keylen;
    expectStr[1] = pkeylen[1];
    expectStr[2] = pkeylen[0];
    memcpy(expectStr+3, key.data(), keylen);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[keylen+3] = pversion[1];
    expectStr[keylen+4] = pversion[0];
    uint64_t uscore = encodeScore(score);
    uint8_t* pscore = (uint8_t*)&uscore;
    for(int i = 0; i < 8; i++)
    {
        expectStr[keylen+i+5] = pscore[7-i];
    }
    memcpy(expectStr+13+keylen, member.data(), member.size());

    EXPECT_EQ(0, key_zscore.compare(0, 13+keylen+member.size(), expectStr, 13+keylen+member.size()));
}

TEST_F(EncodeTest, Test_encode_zscore_key) {
    char* space = new char[maxFieldLen_+maxKeyLen_+13];
    string key, member;
    uint16_t version;
    double score;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        member = GetRandomField_();
        score = GetRandomDouble_();
        version = GetRandomVer_();
        compare_encode_zscore_key( key, member, score, version, space);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_zscore_key(Keys[n], member, score, version, space);

    //MaxLength key
    compare_encode_zscore_key(GetRandomBytes_(maxKeyLen_), member, score, version, space);

    delete space;
}

void compare_encode_list_key(const string& key, uint64_t seq, uint16_t version, char* expectStr){
    string key_list = encode_list_key(key, seq, version);
    expectStr[0] = 'S';
    uint16_t keylen = key.size(); 
    uint8_t* pkeylen = (uint8_t*)&keylen;
    expectStr[1] = pkeylen[1];
    expectStr[2] = pkeylen[0];
    memcpy(expectStr+3, key.data(), keylen);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[keylen+3] = pversion[1];
    expectStr[keylen+4] = pversion[0];
    uint8_t* pseq = (uint8_t*)&seq;
    for(int i = 0; i < 8; i++)
    {
        expectStr[keylen+i+5] = pseq[7-i];
    }

    EXPECT_EQ(0, key_list.compare(0, 13+keylen, expectStr, 13+keylen));
}

TEST_F(EncodeTest, Test_encode_list_key) {
    char* space = new char[1+2+maxKeyLen_+2+8];
    string key;
    uint64_t seq;
    uint16_t version;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        seq = GetRandomSeq_();
        version = GetRandomVer_();
        compare_encode_list_key( key, seq, version, space);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_list_key(Keys[n], seq, version, space);

    //MaxLength key, max sequence
    compare_encode_list_key(GetRandomBytes_(maxKeyLen_), MAX_UINT64, version, space);

    delete space;
}

void compare_encode_kv_val(const string& val, uint16_t version, char del, char* expectStr){
    string val_kv = encode_kv_val(val, version, del);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[0] = 'k';
    expectStr[1] = pversion[1];
    expectStr[2] = pversion[0];
    expectStr[3] = del;

    memcpy(expectStr+4, val.data(), val.size());
    EXPECT_EQ(0, val_kv.compare(0, 4+val.size(), expectStr, 4+val.size()));
}

TEST_F(EncodeTest, Test_encode_kv_val) {
    char* space = new char[1+maxValLen_];
    string val;
    uint16_t version;
    char del;

    int valsNum = 100;
    for(int n = 0; n < valsNum; n++)
    {
        val = GetRandomVal_();
        version = GetRandomVer_();

        del = (n&0x1) == 0?'D':'E';

        compare_encode_kv_val(val, version, del, space);
    }

    //MaxLength val
    compare_encode_kv_val(GetRandomBytes_(maxValLen_), version, del, space);

    delete space;
}

void compare_encode_meta_val_internal(string(*func)(uint64_t, uint16_t, char),const char type, uint64_t length, uint16_t version, char del, char* expectStr){
    string val_internal = (*func)(length, version, del);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[0] = type;
    expectStr[1] = pversion[1];
    expectStr[2] = pversion[0];
    expectStr[3] = del;
    uint8_t* plen = (uint8_t*)&length;
    for(int i = 0; i < 8; i++)
    {
        expectStr[i+4] = plen[7-i];
    }
    EXPECT_EQ(0, val_internal.compare(0, 12, expectStr, 12));
}

TEST_F(EncodeTest, Test_encode_hash_meta_val) {
    char* space = new char[12];
    uint64_t elNum;
    uint16_t version;
    char del;

    for(int n = 0; n < 100; n++)
    {
        elNum = GetRandomUInt64_(0, MAX_UINT64-1);
        version = GetRandomVer_();

        del = (n&0x1) == 0?'D':'E';

        compare_encode_meta_val_internal(encode_hash_meta_val, 'H', elNum, version, del,  space);
    }

    //MaxLength val
    compare_encode_meta_val_internal(encode_hash_meta_val, 'H', MAX_UINT64, version, del,  space);

    delete space;
}

TEST_F(EncodeTest, Test_encode_set_meta_val) {
    char* space = new char[12];
    uint64_t elNum;
    uint16_t version;
    char del;

    for(int n = 0; n < 100; n++)
    {
        elNum = GetRandomUInt64_(0, MAX_UINT64-1);
        version = GetRandomVer_();

        del = (n&0x1) == 0?'D':'E';

        compare_encode_meta_val_internal(encode_set_meta_val, 'S', elNum, version, del,  space);
    }

    //MaxLength val
    compare_encode_meta_val_internal(encode_set_meta_val, 'S', MAX_UINT64, version, del,  space);

    delete space;
}

TEST_F(EncodeTest, Test_encode_zset_meta_val) {
    char* space = new char[12];
    uint64_t elNum;
    uint16_t version;
    char del;

    for(int n = 0; n < 100; n++)
    {
        elNum = GetRandomUInt64_(0, MAX_UINT64-1);
        version = GetRandomVer_();

        del = (n&0x1) == 0?'D':'E';

        compare_encode_meta_val_internal(encode_zset_meta_val, 'Z', elNum, version, del,  space);
    }

    //MaxLength val
    compare_encode_meta_val_internal(encode_zset_meta_val, 'Z', MAX_UINT64, version, del,  space);

    delete space;
}

void compare_encode_list_meta_val(uint64_t length, uint64_t left, uint64_t right, uint16_t version, char del, char* expectStr){
    string val_list = encode_list_meta_val(length, left, right, version, del);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[0] = 'L';
    expectStr[1] = pversion[1];
    expectStr[2] = pversion[0];
    expectStr[3] = del;
    uint8_t* plen = (uint8_t*)&length;
    for(int i = 0; i < 8; i++)
    {
        expectStr[i+4] = plen[7-i];
    }
    uint8_t* pleft = (uint8_t*)&left;
    for(int i = 0; i < 8; i++)
    {
        expectStr[i+12] = pleft[7-i];
    }
    uint8_t* pright = (uint8_t*)&right;
    for(int i = 0; i < 8; i++)
    {
        expectStr[i+20] = pright[7-i];
    }
    EXPECT_EQ(0, val_list.compare(0, 28, expectStr, 28));
}

TEST_F(EncodeTest, Test_encode_list_meta_val) {
    char* space = new char[28];
    uint64_t elNum, left, right;
    uint16_t version;
    char del;

    for(int n = 0; n < 100; n++)
    {
        elNum = GetRandomUInt64_(0, MAX_UINT64-1);
        left = GetRandomUInt64_(0, MAX_UINT64-1);
        right = GetRandomUInt64_(0, MAX_UINT64-1);
        version = GetRandomVer_();

        del = (n&0x1) == 0?'D':'E';

        compare_encode_list_meta_val(elNum, left, right, version, del,  space);
    }

    //MaxLength val
    compare_encode_list_meta_val(MAX_UINT64, MAX_UINT64, MAX_UINT64, version, del, space);

    delete space;
}

void compare_encode_delete_key(const string & key, char key_type, uint16_t version, char* expectStr){
    string delete_key = encode_delete_key(key, version);
    uint16_t keylen = key.size(); 
    uint16_t slot = (uint16_t)keyHashSlot(key.data(), keylen);
    uint8_t* pslot = (uint8_t*)&slot;
    expectStr[0] = 'D';
    expectStr[1] = pslot[1];
    expectStr[2] = pslot[0];
    uint8_t* pkeylen = (uint8_t*)&keylen;
    expectStr[3] = pkeylen[1];
    expectStr[4] = pkeylen[0];
    memcpy(expectStr+5, key.data(), keylen);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[keylen+5] = pversion[1];
    expectStr[keylen+6] = pversion[0];
    EXPECT_EQ(0, delete_key.compare(0, keylen+7, expectStr, keylen+7));
}

TEST_F(EncodeTest, Test_encode_delete_key) {
    char* space = new char[maxKeyLen_+7];
    uint16_t version;

    //Some special keys
    uint16_t keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
    {
        version = GetRandomVer_();
        compare_encode_delete_key(Keys[n], KeyTypes[n%sizeof(KeyTypes)], version,  space);
    }

    //Some random keys
    keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        string key = GetRandomKey_(); 
        compare_encode_delete_key(key, KeyTypes[n%sizeof(KeyTypes)], version, space);
    }

    //MaxLength key
    compare_encode_delete_key(GetRandomBytes_(maxKeyLen_), KeyTypes[0], version, space);

    delete space;
}

void compare_encode_zset_score_prefix(const string & key, uint16_t version, char* expectStr){
    string key_zscore_prefix = encode_zscore_prefix(key, version);
    expectStr[0] = 'z';
    uint16_t keylen = key.size();
    uint8_t* pkeylen = (uint8_t*)&keylen;
    expectStr[1] = pkeylen[1];
    expectStr[2] = pkeylen[0];
    memcpy(expectStr+3, key.data(), keylen);
    uint8_t* pversion = (uint8_t*)&version;
    expectStr[keylen+3] = pversion[1];
    expectStr[keylen+4] = pversion[0];

    EXPECT_EQ(0, key_zscore_prefix.compare(0, 5+keylen, expectStr, 5+keylen));
}

TEST_F(EncodeTest, Test_encode_zset_score_prefix) {
    char* space = new char[maxKeyLen_+5];
    string key;
    uint16_t version;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_();
        version = GetRandomVer_();
        compare_encode_zset_score_prefix(key, version, space);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_zset_score_prefix(Keys[n], version, space);

    //MaxLength key
    compare_encode_zset_score_prefix(GetRandomBytes_(maxKeyLen_), version, space);

    delete space;
}

void compare_encode_escore_key(const string & key, uint64_t ts, char* expectStr){
    string escore_key = encode_escore_key(key, ts);
    expectStr[0] = 'T';
    uint8_t* pts = (uint8_t*)&ts;
    for(int i = 0; i < 8; i++)
    {
        expectStr[i+1] = pts[7-i];
    }
    memcpy(expectStr+9, key.data(), key.size());
    EXPECT_EQ(0, escore_key.compare(0, key.size()+9, expectStr, key.size()+9));
}

TEST_F(EncodeTest, Test_encode_escore_key) {
    char* space = new char[maxKeyLen_+9];

    //Some special keys
    uint16_t keysNum = sizeof(Keys)/sizeof(string);
    uint64_t timestamp;

    for(int n = 0; n < keysNum; n++)
    {
        timestamp = GetRandomUInt64_(0, MAX_UINT64-1);
        compare_encode_escore_key(Keys[n], timestamp, space);
    }

    //Some random keys
    keysNum = 500;
    for(int n = 0; n < keysNum; n++)
    {
        string key = GetRandomKey_();
        compare_encode_escore_key(key, timestamp, space);
    }

    //MaxLength key
    compare_encode_escore_key(GetRandomBytes_(maxKeyLen_), timestamp, space);

    delete space;
}

void compare_encode_eset_key(const string & key, char* expectStr){
    string eset_key = encode_eset_key(key);
    expectStr[0] = 'E';
    memcpy(expectStr+1, key.data(), key.size());
    EXPECT_EQ(0, eset_key.compare(0, key.size()+1, expectStr, key.size()+1));
}

TEST_F(EncodeTest, Test_encode_eset_key) {
    char* space = new char[maxKeyLen_+1];

    //Some special keys
    uint16_t keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_encode_eset_key(Keys[n], space);

    //Some random keys
    keysNum = 500;
    for(int n = 0; n < keysNum; n++)
    {
        string key = GetRandomKey_();
        compare_encode_eset_key(key, space);
    }

    //MaxLength key
    compare_encode_eset_key(GetRandomBytes_(maxKeyLen_), space);

    delete space;
}
