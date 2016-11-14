#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

class KVTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list;
        std::vector<std::string> keys;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
};

TEST_F(KVTest, Test_kv_set) {
#define OKSet s = client->set(key, val);\
    ASSERT_TRUE(s.ok())<<"fail to set key!"<<endl;\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.ok()&&(val == getVal))<<"fail to get key val!"<<endl;

// #define FalseSet s = client->set(key, val);\
    // ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl;
 
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        OKSet
        //set exsit key
        val = GetRandomVal_(); 
        OKSet
    }

    //Some random keys
    keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        OKSet
    }

    //other types key, kv can set other types
    s = client->del(key);
    field = GetRandomField_();
    s = client->hset(field, key, val);
    OKSet

    //MaxLength key
    key = GetRandomBytes_(maxKeyLen_);
    OKSet

    /* key += "K";
    FalseSet */
}

TEST_F(KVTest, Test_kv_setx) {
    uint8_t ttl;
#define OKSetx s = client->setx(key, val, ttl);\
    ASSERT_TRUE(s.ok())<<"fail to set key!"<<endl;\
    sleep(ttl-1);\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.ok()&&(val == getVal))<<"fail to get key val!"<<endl;\
    sleep(2);\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    key = GetRandomBytes_(200); 
    val = GetRandomVal_(); 
    ttl = 2;
    OKSetx

    s = client->set(key, val);
    //set exsit key
    val = GetRandomVal_(); 
    OKSetx
}

TEST_F(KVTest, Test_kv_get) {
#define NotFoundGet s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    //Some random keys
    for(int n = 0; n < 5; n++)
    {
        key = GetRandomKey_(); 
        val = GetRandomVal_(); 
        s = client->del(key);
        NotFoundGet
    }

    //other types key
    s = client->del(key);
    field = GetRandomField_();
    s = client->hset(field, key, val);
    NotFoundGet
}

TEST_F(KVTest, Test_kv_del) {
#define OKDel s = client->del(key);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define NotFoundDel s = client->del(key);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        s = client->set(key, val);
        OKDel
        NotFoundDel
    }

    //Some random keys
    keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        s = client->set(key, val);
        OKDel
        NotFoundDel
    }

    //MaxLength key
    key = GetRandomBytes_(maxKeyLen_);
    s = client->set(key, val);
    OKDel
    NotFoundDel
}

TEST_F(KVTest, Test_kv_incr) {
#define OKIncr incr = GetRandomInt64_();\
    s = client->del(key);\
    s = client->incr(key, incr, &ret);\
    EXPECT_EQ(s.code(),"ok");\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(incr), getVal);\
    \
    s = client->incr(key, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(incr+n), getVal);

    int64_t incr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        OKIncr
    }

    //Some random keys
    keysNum = 100;
    val = ""; 
    for(n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        OKIncr
    }
    s = client->del(key);
    s = client->incr(key, 1, &ret);
    s = client->incr(key, MAX_INT64, &ret);
    ASSERT_TRUE(s.error());
    s = client->get(key, &getVal);
    ASSERT_EQ(1, atoi(getVal.data()));

    s = client->del(key);
    s = client->incr(key, -1, &ret);
    s = client->incr(key, MIN_INT64, &ret);
    ASSERT_TRUE(s.error());
    s = client->get(key, &getVal);
    ASSERT_EQ(-1, atoi(getVal.data()));
}

TEST_F(KVTest, Test_kv_keys) {
    s = client->keys("", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 2);
    list.clear();
    client->set("000000001","");
    client->set("000000002","");
    s = client->keys("000000000", "000000002", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 2);
    ASSERT_EQ("000000001", list[0]);
    ASSERT_EQ("000000002", list[1]);
}

TEST_F(KVTest, Test_kv_scan) {
    s = client->scan("", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->set("00000000k1","v1");
    client->set("00000000k2","v2");
    s = client->scan("00000000k0", "00000000k2", 2, &list);
    ASSERT_EQ(list.size() , 4);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000k1", list[0]);
    ASSERT_EQ("v1", list[1]);
    ASSERT_EQ("00000000k2", list[2]);
    ASSERT_EQ("v2", list[3]);
    list.clear();
    s = client->scan("00000000k2", "00000000k0", 2, &list);
    ASSERT_EQ(0, list.size());
}

TEST_F(KVTest, Test_kv_rscan) {
    s = client->rscan("", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->set("00000000k1","v1");
    client->set("00000000k2","v2");
    s = client->rscan("00000000k3", "00000000k1", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000k2", list[0]);
    ASSERT_EQ("v2", list[1]);
    ASSERT_EQ("00000000k1", list[2]);
    ASSERT_EQ("v1", list[3]);
    list.clear();
    s = client->rscan("00000000k1", "00000000k3", 2, &list);
    ASSERT_EQ(0, list.size());
}

TEST_F(KVTest, Test_kv_multi_set_get_del) {
    string key1, key2, val1, val2, key3, val3;

    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key1 = *it; 
        key2 = key1+'2'; 
        key3 = key1+'3'; 
        val1 = "";//GetRandomVal_();
        val2 = val1+'2';
        val3 = val1+'3';
        kvs.clear();
        keys.clear();
        list.clear();
        kvs.insert(std::make_pair(key1, val1));
        kvs.insert(std::make_pair(key2, val2));
        keys.push_back(key1);
        keys.push_back(key2);
        keys.push_back(key3);

        //all keys not exist
        s = client->multi_del(keys);
        ASSERT_TRUE(s.ok());
        s = client->multi_get(keys, &list);
        ASSERT_EQ(0, list.size());
        s = client->multi_set(kvs);
        ASSERT_TRUE(s.ok());
        s = client->multi_get(keys, &list);
        ASSERT_EQ(4, list.size());
        ASSERT_EQ(key1, list[0]);
        ASSERT_EQ(val1, list[1]);
        ASSERT_EQ(key2, list[2]);
        ASSERT_EQ(val2, list[3]);
        kvs.insert(std::make_pair(key3, val3));

        //one key not exist, two keys exist
        s = client->multi_set(kvs);
        ASSERT_TRUE(s.ok());
        list.clear();
        s = client->multi_get(keys, &list);
        ASSERT_EQ(6, list.size());
        kvs.clear();
        val1 = val1+'1';
        val2 = val2+'2';
        val3 = val3+'3';
        kvs.insert(std::make_pair(key1, val1));
        kvs.insert(std::make_pair(key2, val2));
        kvs.insert(std::make_pair(key3, val3));

        //all keys exist, update their vals
        s = client->multi_set(kvs);
        ASSERT_TRUE(s.ok());
        list.clear();
        s = client->multi_get(keys, &list);
        ASSERT_EQ(6, list.size());
        ASSERT_EQ(key1, list[0]);
        ASSERT_EQ(val1, list[1]);
        ASSERT_EQ(key2, list[2]);
        ASSERT_EQ(val2, list[3]);
        ASSERT_EQ(key3, list[4]);
        ASSERT_EQ(val3, list[5]);
        s = client->multi_del(keys);
        ASSERT_TRUE(s.ok());
        list.clear();
        s = client->multi_get(keys, &list);
        ASSERT_EQ(0, list.size());
    }
}

TEST_F(KVTest, Test_kv_setnx) {
    key = "ksetnx";
    val = "valsetnx";
    s = client->del(key);
    s = client->setnx(key, val);
    ASSERT_TRUE(s.ok());
    s = client->get(key,&getVal);
    ASSERT_EQ(val, getVal);
    s = client->setnx(key, val + "Update");
    ASSERT_TRUE(s.error());
    s = client->get(key,&getVal);
    ASSERT_EQ(val, getVal);
}

TEST_F(KVTest, Test_kv_setbit_getbit) {
    key = "kbit";
    val = "valbit";
    int bitoffset = GetRandomUint_(0, 65535*8);
    // int bitoffset = GetRandomUint_(0, MAX_UINT32-1);
    int64_t getBit;
    s = client->del(key);
    s = client->getbit(key, bitoffset, &getBit);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(0, getBit);
    s = client->setbit(key, bitoffset, 1);
    ASSERT_TRUE(s.ok());
    s = client->getbit(key, bitoffset, &getBit);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(1, getBit);
    s = client->setbit(key, bitoffset, 0);
    ASSERT_TRUE(s.ok());
    s = client->getbit(key, bitoffset, &getBit);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(0, getBit);
    s = client->del(key);
    //"0" is 0x30:0x00110000, and big-little end switch
    s = client->setbit(key, 2, 1);
    s = client->setbit(key, 3, 1);
    s = client->get(key, &getVal);
    ASSERT_EQ("0", getVal);
}

TEST_F(KVTest, Test_kv_getset) {
    key = "kgetset";
    val = "valgetset";
    s = client->del(key);
    s = client->getset(key, val, &getVal);
    ASSERT_TRUE(s.not_found());
    ASSERT_EQ("", getVal);
    s = client->get(key, &getVal);
    ASSERT_EQ(val, getVal);
    string val2 = val + "2";
    s = client->getset(key, val2, &getVal);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, getVal);
    s = client->get(key, &getVal);
    ASSERT_EQ(val2, getVal);
}
