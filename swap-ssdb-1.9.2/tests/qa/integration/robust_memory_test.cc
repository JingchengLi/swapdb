//Check the disk/memory usage, and scan to check the dataset is clear after test
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"

using namespace std;

class DISABLED_RobustMemoryTest : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int ELEMENT_NUM = 100;
        std::vector<std::string> keys;
        string key, val, getVal, field;
        double score;
        int64_t ret;
};

TEST_F(DISABLED_RobustMemoryTest, Test_string_memory) {
    key = "key";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    keys.clear();
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->set(key+itoa(n), val+itoa(n));
        keys.push_back(key+itoa(n));
    }
    client->multi_del(keys);
}

TEST_F(DISABLED_RobustMemoryTest, Test_hset_memory) {
    key = "hashkey";
    field = GetRandomBytes_(1*1024*1024);
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n < ELEMENT_NUM; n++)
    {
        field = field+itoa(n);
        client->hset(key, field, val);
    }
    client->del(key);
}

TEST_F(DISABLED_RobustMemoryTest, Test_list_memory) {
    key = "listkey";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->qpush_front(key, val);
    }
    client->del(key);
}

TEST_F(DISABLED_RobustMemoryTest, Test_set_memory) {
    key = "setkey";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->sadd(key, val+itoa(n));
    }
    client->del(key);
}

TEST_F(DISABLED_RobustMemoryTest, Test_zset_memory) {
    key = "zsetkey";
    field = GetRandomBytes_(1*1024*1024);
    score = GetRandomDouble_();
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->zset(key, field+itoa(n), score+n);
    }
    client->del(key);
}

TEST_F(DISABLED_RobustMemoryTest, Test_expire_string_memory) {
    key = "expirekey";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->set(key+itoa(n), val+itoa(n));
        keys.push_back(key+itoa(n));
    }
    for(vector<string>::iterator it = keys.begin(); it != keys.end(); it++)
        client->expire(*it, 1, &ret);
}

TEST_F(DISABLED_RobustMemoryTest, Test_expire_hset_memory) {
    key = "expirehashkey";
    field = GetRandomBytes_(1*1024*1024);
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n < ELEMENT_NUM; n++)
    {
        field = field+itoa(n);
        client->hset(key, field, val);
    }
    client->expire(key, 1, &ret);
}

TEST_F(DISABLED_RobustMemoryTest, Test_expire_list_memory) {
    key = "expirelistkey";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->qpush_front(key, val);
    }
    client->expire(key, 1, &ret);
}

TEST_F(DISABLED_RobustMemoryTest, Test_expire_set_memory) {
    key = "expiresetkey";
    val = GetRandomBytes_(1*1024*1024);
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->sadd(key, val+itoa(n));
    }
    client->expire(key, 1, &ret);
}

TEST_F(DISABLED_RobustMemoryTest, Test_expire_zset_memory) {
    key = "expirezsetkey";
    field = GetRandomBytes_(1*1024*1024);
    score = GetRandomDouble_();
    client->del(key);
    for(int n = 0; n<ELEMENT_NUM; n++)
    {
        client->zset(key, field+itoa(n), score+n);
    }
    client->expire(key, 1, &ret);
}
