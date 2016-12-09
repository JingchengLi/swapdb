#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"


using namespace std;

class DISABLED_RobustTest_Version : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int VERSION = 65536;
        static const int threadsNum = 9;
        std::vector<std::string> list;
        std::vector<std::string> keys;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
        int64_t ret;
        double score, getScore;


};

TEST_F(DISABLED_RobustTest_Version, Test_string_get_del_version_repeat) {
//30s
    key = "key";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->set(key, val+itoa(n));
        client->get(key, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
        client->del(key);
    }

    for(int n = 0;n < keysNum; n++){
        client->set(key, val+itoa(n));
        client->get(key, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
    }
    client->del(key);
}

TEST_F(DISABLED_RobustTest_Version, Test_string_getset_del_version_repeat) {
    key = "getsetkey";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        s = client->getset(key, val+itoa(n), &getVal);

        ASSERT_TRUE(s.not_found());
        client->del(key);
    }

    for(int n = 0;n < keysNum; n++){
        s = client->getset(key, val+itoa(n), &getVal);
        if(n > 0)
            ASSERT_EQ(val+itoa(n-1), getVal.data());
    }
    client->del(key);
}

TEST_F(DISABLED_RobustTest_Version, Test_string_setbit_del_version_repeat) {
    key = "setbitkey";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    int64_t getBit;
    for(int n = 0;n < keysNum; n++){
        client->setbit(key, 1, n&0x1);
        client->getbit(key, 1, &getBit);

        ASSERT_EQ(n&0x1, getBit);
        client->del(key);
    }

    for(int n = 0;n < keysNum; n++){
        client->setbit(key, 1, n&0x1);
        client->getbit(key, 1, &getBit);
        ASSERT_EQ(n&0x1, getBit);
    }
    client->del(key);
}

TEST_F(DISABLED_RobustTest_Version, Test_list_lpush_get_del_version_repeat) {
    key = "listkey";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->qpush_front(key, val+itoa(n));
        client->qpop_back(key, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
    }

    for(int n = 0;n < keysNum; n++){
        client->qpush_front(key, val+itoa(n));
        client->del(key);
        client->qsize(key, &ret);
        ASSERT_EQ(0, ret)<<endl;
    }
}

TEST_F(DISABLED_RobustTest_Version, Test_list_rpush_get_del_version_repeat) {
    key = "listkey";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->qpush_back(key, val+itoa(n));
        client->qpop_front(key, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
    }

    for(int n = 0;n < keysNum; n++){
        client->qpush_back(key, val+itoa(n));
        client->del(key);
        client->qsize(key, &ret);
        ASSERT_EQ(0, ret)<<endl;
    }
}

TEST_F(DISABLED_RobustTest_Version, Test_set_get_del_version_repeat) {
#define Sismember(TF, val) s = client->sismember(key, val, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to sismember key!"<<endl;\
    ASSERT_EQ(TF, ret)<<":"<<key<<endl;

    key = "setkey";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->sadd(key, val+itoa(n));
        Sismember(1, val+itoa(n))
        client->srem(key, val+itoa(n));
        Sismember(0, val+itoa(n))
    }

    for(int n = 0;n < keysNum; n++){
        client->sadd(key, val+itoa(n));
        client->del(key);
        Sismember(0, val)
    }
}

TEST_F(DISABLED_RobustTest_Version, Test_zset_get_del_version_repeat) {

    key = "zsetkey";
    field = "field";
    score = 1.0;
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->zset(key, field, score+(double)n);
        client->zget(key, field, &getScore);
        ASSERT_EQ(score+(double)n, getScore)<<endl;
        client->zdel(key, field);
    }

    for(int n = 0;n < keysNum; n++){
        client->zset(key, field, score+(double)n);
        client->zget(key, field, &getScore);
        ASSERT_EQ(score+(double)n, getScore)<<endl;
    }
    client->zdel(key, field);
}

TEST_F(DISABLED_RobustTest_Version, Test_hash_get_del_version_repeat) {
    key = "hashkey";
    field = "field";
    val = "val";
    keysNum = VERSION;
    client->del(key);
    for(int n = 0;n < keysNum; n++){
        client->hset(key, field, val+itoa(n));
        client->hget(key, field, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
        client->hdel(key, field);
    }

    for(int n = 0;n < keysNum; n++){
        client->hset(key, field, val+itoa(n));
        client->hget(key, field, &getVal);
        ASSERT_EQ(val+itoa(n), getVal.data())<<endl;
    }
    client->hdel(key, field);
}
