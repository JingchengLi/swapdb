#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

class HashTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list;
        std::map<std::string, std::string> kvs;
        std::vector<std::string> keys;
        string key, val, getVal, field;
        uint16_t keysNum;
        int64_t ret;

    private:
        string ip;
        int port;
};

/* s = client->hset(hash, key, "test_val");
   assert(s.ok());

   s = client->hget(hash, key, &val);
   assert(s.ok() && (val == test_val));
   printf("%s = %s\n", key.c_str(), val.c_str());

   s = client->hdel(hash, key);
   assert(s.ok()); */
TEST_F(HashTest, Test_hash_hset) {
#define OKHset s = client->hset(key, field, val);\
    ASSERT_TRUE(s.ok())<<"fail to hset key!"<<endl;\
    s = client->hget(key, field, &getVal);\
    ASSERT_TRUE(s.ok()&&(val == getVal))<<"fail to hget key val!"<<endl;

#define FalseHset s = client->hset(key, field, val);\
    ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        val = GetRandomVal_(); 
        s = client->del(key);
        OKHset

        // set exsit key
        field = GetRandomField_();
        val = GetRandomVal_(); 
        OKHset
    }

    // Some random keys
    keysNum = 100;
    val = ""; 
    key = GetRandomKey_(); 
    field = GetRandomField_();
    s = client->del(key);
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        OKHset
    }
    s = client->hsize(key, &ret);
    ASSERT_EQ(keysNum, ret);

    //other types key
    s = client->del(key);
    field = GetRandomField_();
    s = client->set(key, val);
    FalseHset
}

TEST_F(HashTest, Test_hash_hget) {
#define NotFoundHget s = client->hget(key, field, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    // Some random keys
    for(int n = 0; n < 5; n++)
    {
        key = GetRandomKey_(); 
        val = GetRandomVal_(); 
        field = GetRandomField_();
        s = client->del(key);
        NotFoundHget
    }

    keysNum = 100;
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        s = client->hset(key, field, val);
    }
    field = field+itoa(keysNum);
    NotFoundHget
}

TEST_F(HashTest, Test_hash_hdel) {
#define OKHdel s = client->hdel(key, field);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    s = client->hget(key, field, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define NotFoundHdel s = client->hdel(key, field);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        field = GetRandomField_();
        s = client->hset(key, field, val);
        OKHdel
        NotFoundHdel
    }

    keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        s = client->hset(key, field, val);
        OKHdel
        NotFoundHdel
    }
}

TEST_F(HashTest, Test_hash_hincr) {
#define OKHincr incr = GetRandomInt64_();\
    s = client->del(key);\
    s = client->hincr(key, field, incr, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->hget(key, field, &getVal);\
    ASSERT_EQ(to_string(incr), getVal);\
    \
    s = client->hincr(key, field, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->hget(key, field, &getVal);\
    ASSERT_EQ(to_string(incr+n), getVal);

    int64_t incr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        field = GetRandomField_();
        OKHincr
    }

    //Some random keys
    keysNum = 10;
    val = ""; 
    for(n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        OKHincr
    }

    s = client->del(key);
    s = client->hincr(key, field, 1, &ret);
    s = client->hincr(key, field, MAX_INT64, &ret);
    ASSERT_TRUE(s.error());
    s = client->hget(key, field, &getVal);
    ASSERT_EQ(1, atoi(getVal.data()));

    s = client->del(key);
    s = client->hincr(key, field, -1, &ret);
    s = client->hincr(key, field, MIN_INT64, &ret);
    ASSERT_TRUE(s.error());
    s = client->hget(key, field, &getVal);
    ASSERT_EQ(-1, atoi(getVal.data()));
}

TEST_F(HashTest, Test_hash_hgetall) {
#define NotFoundHgetall s = client->hgetall(key, &list);\
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<endl;\
    ASSERT_EQ(0, list.size());

    key = GetRandomKey_(); 
    val = GetRandomVal_(); 
    field = GetRandomField_();
    s = client->hclear(key);
    NotFoundHgetall
    keysNum = 10;
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        val = val+itoa(n);
		kvs.insert(std::make_pair(field, val));
        s = client->hset(key, field, val);
    }
    s = client->hgetall(key, &list);
    for(int n = 0; n < keysNum; n += 2)
    {
        EXPECT_EQ(kvs[list[n]], list[n+1]);
    }
    
}

TEST_F(HashTest, Test_hash_hsize) {
#define OKHsize(num) s = client->hsize(key, &ret);\
    ASSERT_EQ(ret, num)<<"fail to hsize key!"<<key<<endl;\
    ASSERT_TRUE(s.ok())<<"hsize key not ok!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        val = GetRandomVal_(); 
        s = client->del(key);
        OKHsize(0)
        s = client->hset(key, field, val);
        OKHsize(1)
    }

    key = GetRandomKey_(); 
    field = GetRandomField_();
    val = GetRandomVal_(); 
    s = client->del(key);
    for(int n = 0; n < 10; n++)
    {
        field = field+itoa(n);
        val = val+itoa(n);
        client->hset(key, field, val);
        OKHsize(n+1)
    }
}

TEST_F(HashTest, Test_hash_hclear) {
#define OKHclear(num) s = client->hclear(key, &ret);\
    ASSERT_EQ(ret , num)<<"fail to hclear key!"<<key<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        val = GetRandomVal_(); 
        s = client->del(key);
        OKHclear(0)
        s = client->hset(key, field, val);
        OKHclear(1)
    }

    key = GetRandomKey_(); 
    field = GetRandomField_();
    val = GetRandomVal_(); 
    s = client->del(key);
    for(int n = 0; n < 10; n++)
    {
        field = field+itoa(n);
        val = val+itoa(n);
        client->hset(key, field, val);
    }
    OKHclear(10)
}

TEST_F(HashTest, Test_hash_hscan) {
    s = client->hscan(key, "", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->del("key");
    client->hset("key", "00000000f1","v1");
    client->hset("key", "00000000f2","v2");
    s = client->hscan("key", "00000000f0", "00000000f2", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000f1", list[0]);
    ASSERT_EQ("v1", list[1]);
    ASSERT_EQ("00000000f2", list[2]);
    ASSERT_EQ("v2", list[3]);
    list.clear();
    s = client->hscan("key", "00000000f2", "00000000f0", 2, &list);
    ASSERT_EQ(0, list.size());
}

TEST_F(HashTest, Test_hash_hrscan) {
    s = client->hrscan(key, "", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->del("key");
    client->hset("key", "00000000f1","v1");
    client->hset("key", "00000000f2","v2");
    s = client->hrscan("key", "00000000f3", "00000000f1", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000f2", list[0]);
    ASSERT_EQ("v2", list[1]);
    ASSERT_EQ("00000000f1", list[2]);
    ASSERT_EQ("v1", list[3]);
    list.clear();
    s = client->hrscan("key", "00000000f1", "00000000f3", 2, &list);
    ASSERT_EQ(0, list.size());
}
/* TEST_F(KVTest, Test_kv_setx) {
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
    set exsit key
        val = GetRandomVal_(); 
    OKSetx
}


TEST_F(KVTest, Test_kv_del) {
#define OKDel s = client->del(key);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define NotFoundDel s = client->del(key);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    Some special keys
        for(int n = 0; n < keysNum; n++)
        {
            key = Keys[n];
            val = GetRandomVal_(); 
            s = client->set(key, val);
            OKDel
                NotFoundDel
        }

    Some random keys
        keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        s = client->set(key, val);
        OKDel
            NotFoundDel
    }

    MaxLength key
        key = GetRandomBytes_(maxKeyLen_);
    s = client->set(key, val);
    OKDel
        NotFoundDel
}

TEST_F(KVTest, Test_kv_incr) {
#define OKIncr incr = rand()%MAX_UINT64;\
    incr = n&1?incr:(-incr);\
    s = client->del(key);\
    s = client->incr(key, incr, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->get(key, &getVal);\
    ASSERT_EQ(incr, atoi(getVal.data()));\
    \
    s = client->incr(key, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->get(key, &getVal);\
    ASSERT_EQ(incr+n, atoi(getVal.data()));
    stringstream stream;

    int64_t incr, ret;
    Some special keys
        for(int n = 0; n < keysNum; n++)
        {
            key = Keys[n]; 
            OKIncr
        }

    Some random keys
        keysNum = 100;
    val = ""; 
    for(int n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        OKIncr
    }
}

TEST_F(KVTest, Test_kv_keys) {
    s = client->keys("", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 2);
    list.clear();
    client->set("000000001","");
    client->set("000000002","");
    s = client->keys("000000000", "000000002", 2, &list);
    ASSERT_TRUE(s.ok());
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
    ASSERT_TRUE(s.ok());
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
    ASSERT_TRUE(s.ok());
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

    for(int n = 0; n < keysNum; n++)
    {
        key1 = Keys[n]; 
        key2 = key1 + '2';
        key3 = key1 + '3'; 
        val1 = GetRandomVal_();
        val2 = val1 + '2';
        val3 = val1 + '3';
        kvs.clear();
        keys.clear();
        list.clear();
        kvs.insert(std::make_pair(key1, val1));
        kvs.insert(std::make_pair(key2, val2));
        keys.push_back(key1);
        keys.push_back(key2);
        keys.push_back(key3);

        // all keys not exist
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

        // one key not exist, two keys exist
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

        all keys exist, update their vals
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
}  */
