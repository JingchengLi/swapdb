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
        std::vector<std::string> keys, kvs2;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
        int64_t ret;
        static const int MAX_PACKET_SIZE = 128 * 1024 *1024;
};

TEST_F(KVTest, Test_kv_set) {
#define OKSet s = client->set(key, val);\
    ASSERT_TRUE(s.ok())<<"fail to set key!"<<key<<endl;\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.ok()&&(val == getVal))<<"fail to get key val!"<<key<<endl;

#define FalseSet s = client->set(key, val);\
    ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl; 
 
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        client->multi_del(key);
        OKSet
        // set exsit key
        val = GetRandomVal_(); 
        OKSet
        s = client->multi_del(key);
    } 

    //Some random keys
    keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        key = "key"+itoa(n);//GetRandomKey_(); 
        client->multi_del(key);
        OKSet
        client->multi_del(key);
    }

    //other types key, kv can not set other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    FalseSet

    client->multi_del(key);
    client->zset(key, field, 1.0);
    FalseSet

    client->multi_del(key);
    client->qpush_front(key, val);
    FalseSet

    client->multi_del(key);
    client->sadd(key, val);
    FalseSet
    s = client->multi_del(key);

    //MaxLength key
    key = GetRandomBytes_(maxKeyLen_);
    OKSet
    s = client->multi_del(key);

    /* key += "K";
    FalseSet */
}

TEST_F(KVTest, Test_kv_setex) {
    int64_t ttl;
#define OKSetx s = client->setx(key, val, ttl);\
    EXPECT_TRUE(s.ok())<<"fail to set key!"<<endl;\
    sleep(ttl-1);\
    s = client->get(key, &getVal);\
    EXPECT_TRUE(s.ok()&&(val == getVal))<<"fail to get key val!"<<endl;\
    sleep(2);\
    s = client->get(key, &getVal);\
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

#define FalseSetx s = client->setx(key, val, ttl);\
    EXPECT_TRUE(s.error())<<"should fail to set key!"<<endl;

    key = GetRandomBytes_(200); 
    val = GetRandomVal_(); 
    ttl = 2;
    OKSetx

    s = client->set(key, val);
    // set exsit key
    val = GetRandomVal_(); 
    OKSetx

    //other types key, kv can not setex other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    FalseSetx

    client->multi_del(key);
    client->zset(key, field, 1.0);
    FalseSetx

    client->multi_del(key);
    client->qpush_front(key, val);
    FalseSetx

    client->multi_del(key);
    client->sadd(key, val);
    FalseSetx
    client->multi_del(key);

    ttl = -1;
    FalseSetx

    ttl = -1;
    s = client->set(key, val);
    FalseSetx

    ttl = 0;
    FalseSetx

    client->multi_del(key);

    s = client->setx(key, val, MAX_INT64/1000);
    EXPECT_TRUE(s.error())<<"should fail to set key with MAX_INT64/1000!"<<s.code()<<endl;
    s = client->get(key, &getVal);
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<s.code()<<endl;
    client->multi_del(key);
}

TEST_F(KVTest, Test_kv_psetex) {
    int64_t pttl;
#define OKSetx s = client->psetx(key, val, pttl);\
    EXPECT_TRUE(s.ok())<<"fail to set key!"<<endl;\
    sleep(pttl/1000-1);\
    s = client->get(key, &getVal);\
    EXPECT_TRUE(s.ok()&&(val == getVal))<<"fail to get key val!"<<endl;\
    sleep(2);\
    s = client->get(key, &getVal);\
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

#define FalseSetx s = client->psetx(key, val, pttl);\
    EXPECT_TRUE(s.error())<<"should fail to set key!"<<endl;

    key = GetRandomBytes_(200); 
    val = GetRandomVal_(); 
    pttl = 2000;
    OKSetx

    s = client->set(key, val);
    // set exsit key
    val = GetRandomVal_(); 
    OKSetx

    //other types key, kv can not setex other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    FalseSetx

    client->multi_del(key);
    client->zset(key, field, 1.0);
    FalseSetx

    client->multi_del(key);
    client->qpush_front(key, val);
    FalseSetx

    client->multi_del(key);
    client->sadd(key, val);
    FalseSetx
    client->multi_del(key);

    pttl = -1;
    FalseSetx

    pttl = -1;
    s = client->set(key, val);
    FalseSetx

    pttl = 0;
    FalseSetx

    client->multi_del(key);

    s = client->psetx(key, val, MAX_INT64);
    EXPECT_TRUE(s.error())<<"should fail to set key with MAX_INT64!"<<s.code()<<endl;
    s = client->get(key, &getVal);
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<s.code()<<endl;
    client->multi_del(key);
}

TEST_F(KVTest, Test_kv_get) {
#define NotFoundGet s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

#define ErrorGet s = client->get(key, &getVal);\
    ASSERT_TRUE(s.error())<<"this key should be error!"<<endl;

    //Some random keys
    for(int n = 0; n < 5; n++)
    {
        key = GetRandomKey_(); 
        val = GetRandomVal_(); 
        s = client->multi_del(key);
        NotFoundGet
    }

    //other types key
    s = client->multi_del(key);
    field = GetRandomField_();
    s = client->hset(key, field, val);
    ErrorGet
    s = client->multi_hdel(key, field);
}

TEST_F(KVTest, Test_kv_del) {
//redis del == ssdb multi_del
#define OKDel s = client->multi_del(key, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    ASSERT_EQ(1, ret)<<"del one key return 1!"<<endl;\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define NotFoundDel s = client->multi_del(key, &ret);\
    ASSERT_EQ(0, ret)<<"del one non-exist key return 0!"<<endl;

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

TEST_F(KVTest, Test_kv_incrby) {
#define OKIncr incr = GetRandomInt64_();\
    s = client->multi_del(key);\
    s = client->incr(key, incr, &ret);\
    EXPECT_EQ(s.code(),"ok");\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(incr), getVal);\
    \
    s = client->incr(key, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(incr+n), getVal);\
    s = client->multi_del(key);

#define FalseIncr s = client->incr(key, 1, &ret);\
    ASSERT_TRUE(s.error())<<"this key should incr error!"<<endl;

    int64_t incr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        OKIncr
        client->multi_del(key);
    }

    //Some random keys
    keysNum = 100;
    val = ""; 
    for(n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        OKIncr
        client->multi_del(key);

    }
    s = client->multi_del(key);
    s = client->incr(key, MAX_INT64, &ret);
    s = client->incr(key, 1, &ret);
    ASSERT_TRUE(s.error());
    s = client->get(key, &getVal);
    ASSERT_EQ(i64toa(MAX_INT64), getVal);

    s = client->multi_del(key);
    s = client->incr(key, MIN_INT64, &ret);
    s = client->incr(key, -1, &ret);
    ASSERT_TRUE(s.error());
    s = client->get(key, &getVal);
    ASSERT_EQ(i64toa(MIN_INT64), getVal);
    s = client->multi_del(key);

    //other types key, kv can not incr other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    FalseIncr

    client->multi_del(key);
    client->zset(key, field, 1.0);
    FalseIncr

    client->multi_del(key);
    client->qpush_front(key, val);
    FalseIncr

    client->multi_del(key);
    client->sadd(key, val);
    FalseIncr
    client->multi_del(key);
}

TEST_F(KVTest, Test_kv_decrby) {
#define OKdecr decr = GetRandomInt64_();\
    s = client->multi_del(key);\
    s = client->decr(key, decr, &ret);\
    EXPECT_EQ(s.code(),"ok");\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(-1*decr), getVal);\
    \
    s = client->decr(key, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->get(key, &getVal);\
    ASSERT_EQ(to_string(-1*(decr+n)), getVal);\
    s = client->multi_del(key);

    int64_t decr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        OKdecr
    }
}

TEST_F(KVTest, DISABLED_Test_kv_keys) {
    s = client->keys("", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 2);
    list.clear();
    client->set("000000001","");
    client->set("000000002","");
    s = client->keys("000000000", "000000002", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 2);
    ASSERT_EQ("000000001", list[0]);
    ASSERT_EQ("000000002", list[1]);
    client->multi_del("000000001");
    client->multi_del("000000002");
}

TEST_F(KVTest, DISABLED_Test_kv_scan) {
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
    client->multi_del("00000000k1");
    client->multi_del("00000000k2");
}

TEST_F(KVTest, DISABLED_Test_kv_rscan) {
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
    client->multi_del("00000000k1");
    client->multi_del("00000000k2");
}

TEST_F(KVTest, Test_kv_mset_mget_del) {
//redis mset/mget/del
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
        s = client->multi_del(keys, &ret);
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
        s = client->multi_del(keys, &ret);
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(3, ret);
        list.clear();
        s = client->multi_get(keys, &list);
        ASSERT_EQ(0, list.size());

        //Should set error when there is some key can not be set
        for( int n = 0; n< 4; n++ ){

            client->multi_del(key2);
            switch(n){
                case 0:
                    client->hset(key2, "field", val2);
                    break;
                case 1:
                    client->qpush_front(key2, val2);
                    break;
                case 2:
                    client->sadd(key2, val2);
                    break;
                case 3:
                    client->zset(key2, "field", 1.0);
                    break;
            }

            s = client->multi_set(kvs);
            EXPECT_EQ("error", s.code())<<n;
            list.clear();
            s = client->multi_get(keys, &list);
            ASSERT_EQ(0, list.size());
            client->multi_del(key2);
        }
    }

    //Del all types key at one time
    keys.clear();
    keys.push_back("KVKey");
    client->set("KVKey", "KVVal");
    keys.push_back("HashKey");
    client->hset("HashKey", "field", "HashVal");
    keys.push_back("ListKey");
    client->qpush_front("ListKey", "ListVal");
    keys.push_back("SetKey");
    client->sadd("SetKey", "SetVal");
    keys.push_back("ZsetKey");
    client->zset("ZsetKey", "ZsetVal", 1.0);
    client->multi_del(keys, &ret);
    ASSERT_EQ(5, ret);
    client->multi_del(keys, &ret);
    ASSERT_EQ(0, ret);

}

TEST_F(KVTest, Test_kv_setnx) {
    key = "ksetnx";
    val = "valsetnx";
    s = client->multi_del(key);
    s = client->setnx(key, val, &ret);
    s = client->get(key,&getVal);
    ASSERT_EQ(val, getVal);
    s = client->setnx(key, val + "Update", &ret);
    s = client->get(key,&getVal);
    ASSERT_EQ(val, getVal);
    s = client->multi_del(key);
}

TEST_F(KVTest, Test_kv_setbit_getbit) {
    key = "kbit";
    val = "valbit";
    int64_t bitoffset = GetRandomUint_(0, MAX_UINT32-1);
    int64_t getBit;
    s = client->multi_del(key);
    s = client->getbit(key, bitoffset, &getBit);
    EXPECT_TRUE(s.ok())<<s.code();
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
    s = client->multi_del(key);
    //"0" is 0x30:0x00110000, and big-little end switch
    s = client->setbit(key, 2, 1);
    s = client->setbit(key, 3, 1);
    s = client->get(key, &getVal);
    ASSERT_EQ("0", getVal);
    s = client->getbit(key, 9, &getBit);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(0, getBit);
    s = client->multi_del(key);

    //other types key, kv can not set other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    s = client->setbit(key, 1, 1);
    ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl; 
    client->multi_del(key);

    bitoffset = MAX_UINT32;
    s = client->setbit(key, bitoffset, 1);
    ASSERT_EQ("ok",s.code())<<"this key should set ok!"<<endl; 
    s = client->getbit(key, bitoffset, &getBit);
    ASSERT_EQ(1,getBit)<<"should get 1!"<<endl; 

    client->multi_del(key);

    //offset exceed range [0,MAX_PACKET_SIZE]
    s = client->setbit(key, -1, 1);
    ASSERT_NE("ok",s.code())<<"this key should set fail!"<<endl; 
    bitoffset += 1;
    s = client->setbit(key, bitoffset, 1);
    ASSERT_NE("ok",s.code())<<"this key should set fail!"<<endl; 
    s = client->getbit(key, bitoffset, &getBit);
    ASSERT_NE("ok",s.code())<<"this key should get fail!"<<endl;
}

TEST_F(KVTest, Test_kv_getset) {
    key = "kgetset";
    val = "valgetset";
    s = client->multi_del(key);
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
    s = client->multi_del(key);

    //other types key, kv can not set other types
    field = GetRandomField_();

    client->multi_del(key);
    client->hset(key, field, val);
    s = client->getset(key, val+"null", &getVal);\
    ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl; 
    client->multi_del(key);
}

TEST_F(KVTest, Test_kv_samekeys_mset_mget_del) {
//for mset/del same keys one time deadlock issue.
    key = "kkey_";
    val = "kval_";
    keysNum = 20;
    kvs2.clear();
    keys.clear();
    for(int m = 0;m < 2;m++){
        for(int n = 0;n < keysNum;n++) {
            keys.push_back(key+itoa(n));
            kvs2.push_back(key+itoa(n));
            kvs2.push_back(val+itoa(n));
        }
    }
    //all keys not exist
    s = client->multi_del(keys, &ret);
    ASSERT_TRUE(s.ok())<<s.code()<<endl;
    ASSERT_EQ(0, ret);
    list.clear();
    s = client->multi_get(keys, &list);
    ASSERT_EQ(0, list.size());
    //same key same value mset
    s = client->multi_set(kvs2);
    ASSERT_TRUE(s.ok());
    s = client->multi_get(keys, &list);
    ASSERT_EQ(keys.size()*2, list.size());

    for(int n = 0;n < 2*keysNum;n++) {
        ASSERT_EQ(key+itoa(n%keysNum), list[2*n]);
        ASSERT_EQ(val+itoa(n%keysNum), list[2*n+1]);
    }

    //same key diff value mset
    for(int n = 0;n < keysNum;n++) {
        keys.push_back(key+itoa(n));
        kvs2.push_back(key+itoa(n));
        kvs2.push_back(val+itoa(n*2));
    }
    s = client->multi_set(kvs2);
    ASSERT_TRUE(s.ok());
    list.clear();
    s = client->multi_get(keys, &list);
    ASSERT_EQ(keys.size()*2, list.size());

    for(int n = 0;n < 3*keysNum;n++) {
        ASSERT_EQ(key+itoa(n%keysNum), list[2*n]);
        ASSERT_EQ(val+itoa(n%keysNum*2), list[2*n+1]);
    }

    client->multi_del(keys, &ret);
    ASSERT_EQ(keysNum, ret);
}
