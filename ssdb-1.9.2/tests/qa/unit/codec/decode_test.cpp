#include "gtest/gtest.h"
#include "ssdb/const.h"
#include "decode.h"
#include "encode.h"
#include "ssdb_test.h"
#include <stdlib.h>
using namespace std;

class DecodeTest : public SSDBTest
{
};

void compare_meta_key(const string & key){
    string meta_key = encode_meta_key(key);
    uint16_t slot = (uint16_t)keyHashSlot(key.data(), (int)key.size());
    MetaKey metakey;
    EXPECT_EQ(0, metakey.DecodeMetaKey(meta_key));
    EXPECT_EQ(slot, metakey.slot);
    EXPECT_EQ(0, key.compare(metakey.key));
}

TEST_F(DecodeTest, Test_DecodeMetaKey) {

    //Some special keys
    uint16_t keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_meta_key(Keys[n]);

    //Some random keys
    keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        string key = GetRandomKey_(); 
        compare_meta_key(key);
    }

    //MaxLength key
    compare_meta_key(GetRandomBytes_(maxKeyLen_));

    //error return
    string dummyMetaKey [] = {"", "XXXXXXXXX", "M1"};
    MetaKey metakey;
    for(int n = 0; n < sizeof(dummyMetaKey)/sizeof(string); n++)
        EXPECT_EQ(-1, metakey.DecodeMetaKey(dummyMetaKey[n]));
}

void compare_ItemKey(const string & key, const string & field, uint16_t version){
    string item_key;
    switch (rand()%3){
        case 0:
            item_key = encode_hash_key(key, field, version);
            break;
        case 1:
            item_key = encode_set_key(key, field, version);
            break;
        case 2:
            item_key = encode_zset_key(key, field, version);
            break;
    }
    ItemKey itemkey;

    EXPECT_EQ(itemkey.DecodeItemKey(item_key), 0);
    EXPECT_EQ(0, key.compare(itemkey.key));
    EXPECT_EQ(0, field.compare(itemkey.field));
    EXPECT_EQ(version, itemkey.version);
}

TEST_F(DecodeTest, Test_DecodeItemKey) {

    string key, field; 
    uint16_t version;
    // Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_(); 
        version = GetRandomVer_(); 
        compare_ItemKey(key, field, version);
    }

    // Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_ItemKey(Keys[n], field, version);

    // MaxLength key
    compare_ItemKey(GetRandomBytes_(maxKeyLen_), field, version);

    // error return
    string dummyItemKey [] = {"", "X112233445566", "S1", "S112", "S11223"};
    ItemKey itemkey;
    for(int n = 0; n < sizeof(dummyItemKey)/sizeof(string); n++)
        EXPECT_EQ(-1, itemkey.DecodeItemKey(dummyItemKey[n]));
} 

void compare_ZScoreItemKey(const string & key, const string & member, double score, uint16_t version){
    string zscore_item_key;
    zscore_item_key = encode_zscore_key(key, member, score, version);

    ZScoreItemKey zscoreitemkey;

    EXPECT_EQ(zscoreitemkey.DecodeItemKey(zscore_item_key), 0);
    EXPECT_EQ(0, key.compare(zscoreitemkey.key));
    EXPECT_EQ(0, member.compare(zscoreitemkey.field));
    EXPECT_NEAR(score, zscoreitemkey.score, 0.00001);
    EXPECT_EQ(version, zscoreitemkey.version);
}

TEST_F(DecodeTest, Test_ZScore_DecodeItemKey) {

    string key, member; 
    double score;
    uint16_t version;

    // Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < 5; n++)
    {
        key = GetRandomKey_(); 
        member = GetRandomField_(); 
        score = GetRandomDouble_();
        version = GetRandomVer_(); 
        compare_ZScoreItemKey(key, member, score, version);
    }

    // Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_ZScoreItemKey(Keys[n], member, score, version);

    // MaxLength key
    compare_ZScoreItemKey(GetRandomBytes_(maxKeyLen_), member, score, version);

    // error return
    string dummyItemKey [] = {"", "XXXXXXXXX", "z1", "z112", "z11223"};
    ZScoreItemKey zscoreitemkey;
    for(int n = 0; n < sizeof(dummyItemKey)/sizeof(string); n++)
        EXPECT_EQ(-1, zscoreitemkey.DecodeItemKey(dummyItemKey[n]));
} 

void compare_ListItemKey(const string & key, uint64_t seq, uint16_t version){
    string item_key;
    item_key = encode_list_key(key, seq, version);

    ListItemKey listitemkey;

    EXPECT_EQ(listitemkey.DecodeItemKey(item_key), 0);
    EXPECT_EQ(0, key.compare(listitemkey.key));
    EXPECT_EQ(seq, listitemkey.seq);
    EXPECT_EQ(version, listitemkey.version);
}

TEST_F(DecodeTest, Test_List_DecodeItemKey) {

    string key; 
    uint64_t seq;
    uint16_t version;

    // Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        seq = GetRandomSeq_();
        version = GetRandomVer_(); 
        compare_ListItemKey(key, seq, version);
    }

    // Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_ListItemKey(Keys[n], seq, version);

    // MaxLength key
    compare_ListItemKey(GetRandomBytes_(maxKeyLen_), seq, version);

    // error return
    string dummyItemKey [] = {"", "XXXXXXXXX", "S1", "S112", "S11223"};
    ListItemKey listitemkey;
    for(int n = 0; n < sizeof(dummyItemKey)/sizeof(string); n++)
        EXPECT_EQ(-1, listitemkey.DecodeItemKey(dummyItemKey[n]));
} 

void compare_KvMetaVal(const string & val, uint16_t version, char del){
    string meta_val;
    meta_val = encode_kv_val(val, version, del);

    KvMetaVal metaval;

    EXPECT_EQ(metaval.DecodeMetaVal(meta_val), 0);
    EXPECT_EQ('k', metaval.type);
    EXPECT_EQ(version, metaval.version);
    EXPECT_EQ(del, metaval.del);
    if(del == 'E')
        EXPECT_EQ(0, val.compare(metaval.value));
}

TEST_F(DecodeTest, Test_Kv_DecodeMetaVal) {
    string val; 
    char del;
    uint16_t version;

    for(int n = 0; n < 100; n++)
    {
        val = GetRandomVal_();
        del = (n&0x1) == 0?'D':'E';
        version = GetRandomVer_();
        compare_KvMetaVal(val, version, del);
    }

    compare_KvMetaVal("", 0, 'D');
    //
    // error return
    string dummyMetaVal [] = {"","k1", "X00", "X00Eval","k00Xval"};
    KvMetaVal metaval;
    for(int n = 0; n < sizeof(dummyMetaVal)/sizeof(string); n++)
        EXPECT_EQ(-1, metaval.DecodeMetaVal(dummyMetaVal[n]));
} 

void compare_MetaVal(uint64_t length, uint16_t version, char del){
    string meta_val;
    char type;
    switch (rand()%3){
        case 0:
            meta_val = encode_hash_meta_val(length, version, del);
            type = 'H';
            break;
        case 1:
            meta_val = encode_set_meta_val(length, version, del);
            type = 'S';
            break;
        case 2:
            meta_val = encode_zset_meta_val(length, version, del);
            type = 'Z';
            break;
    }

    MetaVal metaval;

    EXPECT_EQ(metaval.DecodeMetaVal(meta_val), 0);
    EXPECT_EQ(del, metaval.del);
    EXPECT_EQ(type, metaval.type);
    EXPECT_EQ(version, metaval.version);
    if(del == 'E')
        EXPECT_EQ(length, metaval.length);
}

TEST_F(DecodeTest, Test_DecodeMetaVal) {
    char del;
    uint16_t version;
    uint64_t length;

    for(int n = 0; n < 100; n++)
    {
        del = (n&0x1) == 0?'D':'E';
        version = GetRandomVer_();
        length = GetRandomUInt64_(0, MAX_UINT64-1);
        compare_MetaVal(length, version, del);
    }

    // error return
    string dummyMetaVal [] = {"", "S1", "Z11", "X11E00010000", "H11X00010000", "H11E01"};
    MetaVal metaval;
    for(int n = 0; n < sizeof(dummyMetaVal)/sizeof(string); n++)
        EXPECT_EQ(-1, metaval.DecodeMetaVal(dummyMetaVal[n]))<<dummyMetaVal[n];
} 


void compare_List_MetaVal(uint64_t length, uint64_t left_seq, uint64_t right_seq, uint16_t version, char del){
    string meta_val;
    meta_val = encode_list_meta_val(length, left_seq, right_seq, version, del);

    ListMetaVal metaval;

    EXPECT_EQ(metaval.DecodeMetaVal(meta_val), 0);
    EXPECT_EQ(del, metaval.del);
    EXPECT_EQ('L', metaval.type);
    EXPECT_EQ(version, metaval.version);
    if(del == 'E')
    {
        EXPECT_EQ(length, metaval.length);
        EXPECT_EQ(left_seq, metaval.left_seq);
        EXPECT_EQ(right_seq, metaval.right_seq);
    }
}

TEST_F(DecodeTest, Test_List_DecodeMetaVal) {
    char del;
    uint16_t version;
    uint64_t length, left_seq, right_seq;

    for(int n = 0; n < 100; n++)
    {
        del = n&0x1?'D':'E';
        version = GetRandomVer_();
        length = GetRandomUInt64_(0, MAX_UINT64-1);
        left_seq = GetRandomUInt64_(0, MAX_UINT64-1);
        right_seq = GetRandomUInt64_(0, MAX_UINT64-1);
        compare_List_MetaVal(length, left_seq, right_seq, version, del);
    }

    // error return
    string dummyMetaVal [] = {"", "L1", "L11", "X11E000100002222222233333333", "L11X000100002222222233333333", "L11E00010", \
        "L11E000100002222222", "L11E00010000222222223333333"};
    ListMetaVal metaval;
    for(int n = 0; n < sizeof(dummyMetaVal)/sizeof(string); n++)
        EXPECT_EQ(-1, metaval.DecodeMetaVal(dummyMetaVal[n]))<<dummyMetaVal[n];
} 

void compare_DeleteKey(const string &key, uint16_t version){
    string delete_key;
    delete_key = encode_delete_key(key, version);

    uint16_t slot = (uint16_t)keyHashSlot(key.data(), (int)key.size());
    DeleteKey deletekey;

    EXPECT_EQ(deletekey.DecodeDeleteKey(delete_key), 0);
    EXPECT_EQ('D', deletekey.type);
    EXPECT_EQ(version, deletekey.version);
    EXPECT_EQ(0, key.compare(deletekey.key));
    EXPECT_EQ(slot, deletekey.slot);
}

TEST_F(DecodeTest, Test_DeleteKey) {
    uint16_t version;
    string key;

    //Some random keys
    uint16_t keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        version = GetRandomVer_();
        compare_DeleteKey(key, version);
    }

    //Some special keys
    keysNum = sizeof(Keys)/sizeof(string);

    for(int n = 0; n < keysNum; n++)
        compare_DeleteKey(Keys[n], version);

    //MaxLength key
    compare_DeleteKey(GetRandomBytes_(maxKeyLen_), version);

    //error return
    string true_key = encode_delete_key(key, version);
    string dummyDelKey [] = {"", "X11", "D1", "D111111"};
    DeleteKey deletekey;
    for(int n = 0; n < sizeof(dummyDelKey)/sizeof(string); n++)
        EXPECT_EQ(-1, deletekey.DecodeDeleteKey(dummyDelKey[n]));
}
