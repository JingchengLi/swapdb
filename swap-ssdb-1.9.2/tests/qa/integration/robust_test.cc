#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
#include <time.h>

using namespace std;

class RobustTest : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int BIG_KEY_NUM = 10000;
        static const int threadsNum = 50;
        std::vector<std::string> list;
        std::vector<std::string> keys;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
        int64_t ret;
        double score, getScore;
        static const uint32_t counts = 100;

    void fillMasterData();
    void checkDataOK(int);

};

TEST_F(RobustTest, Test_bigkey_hash_del) {
#define HsetBigKey(num) for(int n = 0; n < num; n++)\
    {\
        client->hset(key, field+itoa(n), val+itoa(n));\
    }\
    client->del(key);

    key = "hash_key";
    field = "field";
    val = GetRandomBytes_(1*1024);
    s = client->del(key);
    for(int count = 0; count < 1; count++){
        keysNum = BIG_KEY_NUM + count;
        HsetBigKey(keysNum)

        s = client->qpush_front(key, "lfield", &ret);
        EXPECT_EQ(ret, 1)<<"lpush!"<<endl;
        EXPECT_EQ(s.code(), "ok")<<"lpush fail when keys num is "<<keysNum<<endl;
        s = client->qpop_back(key, &getVal);
        EXPECT_EQ("lfield", getVal)<<"qpop val get not equal:"<<key<<endl;
        client->del(key);

        HsetBigKey(keysNum)

        s = client->hset(key, "hfield", "hval");
        EXPECT_EQ(s.code(), "ok")<<"hset fail when keys num is "<<keysNum<<endl;
        s = client->hget(key, "hfield", &getVal);
        EXPECT_TRUE(s.ok()&&("hval" == getVal))<<"fail to hget key val!"<<endl;
        s = client->del(key);

        HsetBigKey(keysNum)

        s = client->sadd(key, "smember");
        EXPECT_EQ(s.code(), "ok")<<"sadd fail when keys num is "<<keysNum<<endl;
        s = client->sismember(key, "smember", &ret);
        EXPECT_EQ(1, ret)<<":"<<key<<"not a member!"<<endl;
        s = client->del(key);

        HsetBigKey(keysNum)

        s = client->zset(key, "zmember", 1.0);
        EXPECT_EQ(s.code(), "ok")<<"zset fail when keys num is "<<keysNum<<endl;
        s = client->zget(key, "zmember", &getScore);
        EXPECT_TRUE(s.ok())<<"fail to zget key score!"<<endl;
        EXPECT_NEAR(1.0, getScore, eps);
        s = client->del(key);

        HsetBigKey(keysNum)

        s = client->set(key, "kval");
        EXPECT_EQ(s.code(), "ok")<<"set fail when keys num is "<<keysNum<<endl;
        s = client->get(key, &getVal);
        ASSERT_TRUE(s.ok()&&("kval" == getVal))<<"fail to get key val!"<<endl;
        s = client->del(key);
    }
}

TEST_F(RobustTest, Test_del_mset_mget_repeat) {
//(123186 ms)
//(181424 ms)
    key = "mkey";
    val = "val";
    keysNum = 20000;
    keys.clear();
    kvs.clear();
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(key+itoa(n));
        kvs.insert(std::make_pair(key+itoa(n), val+itoa(n)));
    }

    s = client->multi_del(keys, &ret);
    if(!s.ok())
        cout<<"del fail!"<<s.code()<<endl;

    s = client->multi_set(kvs);

    list.clear();
    s = client->multi_get(keys, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->get(key+itoa(n), &getVal);
        ASSERT_EQ(val+itoa(n), getVal)<<"fail to set key:"<<endl;
    } 

    s = client->multi_del(keys);
    ASSERT_EQ(keysNum, list.size()/2)<<"Set fail at last:"<<list.size()/2<<endl;
}

TEST_F(RobustTest, Test_multi_bigkey_del) {
//This case need check cpu usage for the background delete thread.
//(60512 ms)
    val = "val";
    field = "field";
    score = 1.0;

    keys.clear();

    for(int count = 0; count < 10; count++){

        key = "zsetkey"+itoa(count);
        keys.push_back(key);
        for(int n = 0; n < BIG_KEY_NUM; n++)
        {
            score += 1; 
            client->zset(key, field+itoa(n), score);
        }
        
        key = "setkey"+itoa(count);
        keys.push_back(key);
        for(int n = 0; n < BIG_KEY_NUM; n++)
        {
            client->sadd(key, val+itoa(n));
        }

        key = "listkey"+itoa(count);
        keys.push_back(key);
        for(int n = 0; n < BIG_KEY_NUM; n++)
        {
            client->qpush_front(key, val+itoa(n));
        }


        key = "hashkey"+itoa(count);
        keys.push_back(key);
        for(int n = 0; n < BIG_KEY_NUM; n++)
        {
            client->hset(key, field+itoa(n), val+itoa(n));
        }
    }

    s = client->multi_del(keys);
    keys.clear();
    ASSERT_TRUE(s.ok())<<"del fail!"<<endl;
}

//zclear api disable
/* TEST_F(RobustTest, Test_zset_del_zclear_block) {
//Issue:For zset key, del and then zclear will block forever.
    key = "key";
    field = "field";
    score = 1.0;
    client->zset(key, field, score);

    client->del(key);
    client->zclear(key, &ret);
    ASSERT_EQ(ret, 0)<<"fail to zclear key!"<<key<<endl;
} */

TEST_F(RobustTest, Test_zset_del_set_rank_repeat) {
//Issue:For repeat del and zset, same members can be set.
//./integ-test --gtest_filter=*rank_repeat  --gtest_shuffle --gtest_break_on_failure --gtest_repeat=20 
    key = "zkey";
    field = "field";
    score = GetRandomDouble_();
    client->del(key);
    for(int repeat = 0; repeat < 1000; repeat++)
    {
        client->del(key);
        int count = 10;

        for(int n = 0; n < count; n++)
        {
            client->zset(key, field + itoa(n), score + n);
        }

        for(int n = 0; n < count; n++)
        {
            // s = client->zrrank(key, field + itoa(n), &ret);
            // ASSERT_EQ(count-n-1, ret);
            s = client->zrank(key, field + itoa(n), &ret);
            ASSERT_EQ(n, ret);
        }
        client->del(key);
    }
}

TEST_F(RobustTest, DISABLED_Test_zset_rrank_time_compare) {
//Issue:For zrrank slow, compare zrank to zrrank, run this case repeat make the zrrank slower.
//Disable this case for it's storage engine's issue
    time_t cur_seconds, pre_seconds;
    int count = 10;

    key = "zrank";
    field = "field";
    score = GetRandomDouble_();

    client->del(key);

    for(int n = 0; n < count; n++)
    {
        client->zset(key, field + itoa(n), score + n);
    }

    key = "zkey";
    field = "field";
    score = GetRandomDouble_();

    client->del(key);

    for(int n = 0; n < count; n++)
    {
        client->zset(key, field + itoa(n), score + n);
    }

    cur_seconds = time((time_t*)NULL);
    for(int repeat = 0; repeat < 1000; repeat++)
    {

        for(int n = 0; n < count; n++)
        {
            s = client->zrank(key, field + itoa(n), &ret);
            ASSERT_EQ(n, ret);
        }
    }
    pre_seconds = cur_seconds;
    cur_seconds = time((time_t*)NULL);
    cout<<"zrank cost total time is:"<<cur_seconds-pre_seconds<<"(secs)"<<endl;

    for(int repeat = 0; repeat < 1000; repeat++)
    {

        for(int n = 0; n < count; n++)
        {
            s = client->zrrank(key, field + itoa(n), &ret);
            ASSERT_EQ(count-n-1, ret);
        }
    }
    pre_seconds = cur_seconds;
    cur_seconds = time((time_t*)NULL);
    cout<<"zrrank cost total time is:"<<cur_seconds-pre_seconds<<"(secs)"<<endl;
    client->del(key);
    client->del("zrank");
}

void RobustTest::fillMasterData(){
    keys.clear();
    for (int n = 0; n < counts; ++n) {
        //string type
        key = "kkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->set(key + "_" +itoa(m), val+itoa(m));
            keys.push_back(key+ "_"+itoa(m));
        }

        //hash type
        key = "hkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->hset(key, field+itoa(m), val+itoa(m));
        }
        keys.push_back(key);

        //list type
        key = "lkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->qpush_back(key, val+itoa(m), &ret);
        }
        keys.push_back(key);

        //set type
        key = "skey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->sadd(key, val+itoa(m), &ret);
        }
        keys.push_back(key);

        //zset type
        key = "zkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->zset(key, field+itoa(m), score+m);
        }
        keys.push_back(key);
    }
}

void RobustTest::checkDataOK(int times=10) {

    for (int n = 0; n < counts; ++n) {
        //string type
        key = "kkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            client->get(key+ "_"+itoa(m), &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<key+"_"+itoa(m);
        }

        //hash type
        key = "hkey_"+itoa(n);
        client->hsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave hsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            client->hget(key, field+itoa(m), &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<key<<":"<<m;
        }

        //list type
        key = "lkey_"+itoa(n);
        client->qsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave qsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            client->qget(key, m, &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<key<<":"<<m;
        }

        //set type
        key = "skey_"+itoa(n);
        client->scard(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave scard error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            client->sismember(key, val+itoa(m), &ret);
            ASSERT_EQ(true, ret)<<key<<":"<<m;
        }

        //zset type
        key = "zkey_"+itoa(n);
        client->zsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave zsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            client->zget(key, field+itoa(m), &getScore);
            ASSERT_NEAR(m+score, getScore, eps)<<key<<":"<<m;
        }
    }
}

TEST_F(RobustTest, Test_del_and_check){
    keysNum = 100;
    fillMasterData();
    checkDataOK();
    client->multi_del(keys);
}
