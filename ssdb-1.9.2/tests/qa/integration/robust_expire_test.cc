#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"

using namespace std;

class RobustTest_Expire : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int BIG_KEY_NUM = 10000;
        static const int threadsNum = 9;
        std::vector<std::string> list;
        std::vector<std::string> keys;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
        int64_t ret;
        double score, getScore;
        uint8_t ttl;
};

TEST_F(RobustTest_Expire, Test_setex_set_del_1) {
//setx, then set before expire
    key = "setexkey"; 
    val = "val"; 
    ttl = 2;
    client->del(key);

    client->setx(key, val, ttl);
    client->ttl(key, &ret);
    ASSERT_NEAR(2, ret, 1)<<"ttl should set to ttl by setex!"<<endl;

    client->setx(key, val, ttl*2);
    client->ttl(key, &ret);
    ASSERT_NEAR(4, ret, 1)<<"ttl should change to ttl*2 by setex!"<<endl;

    s = client->set(key, val);
    client->ttl(key, &ret);
    ASSERT_EQ(-1, ret)<<"ttl should be -1(forever) when set!"<<endl;
    sleep(2);

    s = client->get(key, &getVal);
    ASSERT_EQ("ok", s.code())<<"get should return ok!"<<endl;
    ASSERT_EQ(val, getVal)<<"fail to get key val!"<<endl;

    client->del(key);
}

TEST_F(RobustTest_Expire, Test_setex_set_del_2) {
//setx, then set after expire, then setx
    key = "setexkey"; 
    val = "val"; 
    ttl = 1;
    client->del(key);

    client->setx(key, val, ttl);

    sleep(2);

    s = client->get(key, &getVal);
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    s = client->set(key, val);
    client->ttl(key, &ret);
    ASSERT_EQ(-1, ret)<<"ttl should be -1(forever) when set!"<<endl;

    client->setx(key, val, ttl);

    sleep(2);

    s = client->get(key, &getVal);
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    client->del(key);
}

TEST_F(RobustTest_Expire, Test_setex_set_del_3) {
//setx, then del before expire, then set
    key = "setexkey"; 
    val = "val"; 
    ttl = 10;
    client->del(key);

    client->setx(key, val, ttl);
    client->del(key);
    s = client->get(key, &getVal);
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    s = client->set(key, val);
    s = client->get(key, &getVal);
    ASSERT_EQ("ok", s.code())<<"get should return ok!"<<endl;
    ASSERT_EQ(val, getVal)<<"fail to get key val!"<<endl;
    client->ttl(key, &ret);
    ASSERT_EQ(-1, ret)<<"ttl should be -1(forever) when set!"<<endl;
    client->del(key);
}
