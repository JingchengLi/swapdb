#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"

//log info when set true
const bool LDEBUG = false;

using namespace std;

class DISABLED_RobustTest : public SSDBTest
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
        pthread_t bg_tid[threadsNum];

        static void* mset_thread_func(void *arg);
        static void* mget_thread_func(void *arg);

};

void* DISABLED_RobustTest::mset_thread_func(void *arg) {
    if(LDEBUG)
        cout<<"mset_thread_func start!"<<endl; 
    DISABLED_RobustTest* robustTest = (DISABLED_RobustTest*)arg;
    ssdb::Client *tmpclient = ssdb::Client::connect(robustTest->ip.data(), robustTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in mset_thread_func!";
        return (void *)NULL;
    }

    robustTest->s = tmpclient->multi_set(robustTest->kvs);
    if(!robustTest->s.ok())
        cout<<"set fail!"<<robustTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    if(LDEBUG)
        cout<<"mset_thread_func complete!"<<endl; 
    return (void *)NULL;
}

void* DISABLED_RobustTest::mget_thread_func(void *arg) {
    if(LDEBUG)
        cout<<"mget_thread_func start!"<<endl; 
    DISABLED_RobustTest* robustTest = (DISABLED_RobustTest*)arg;
    ssdb::Client *tmpclient = ssdb::Client::connect(robustTest->ip.data(), robustTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in mget_thread_func!";
        return (void *)NULL;
    }

    robustTest->s = tmpclient->get("key1", &robustTest->getVal);
    if(LDEBUG)
        cout<<"get key1:"<<robustTest->getVal<<endl;

    std::vector<std::string> tmplist;
    tmplist.clear();
    robustTest->s = tmpclient->multi_get(robustTest->keys, &tmplist);
    if(!robustTest->s.ok())
        cout<<"set fail!"<<robustTest->s.code()<<endl;
    if(LDEBUG)
        cout<<"Get list size is:"<<tmplist.size()/2<<endl;


    delete tmpclient;
    tmpclient = NULL;
    if(LDEBUG)
        cout<<"mget_thread_func end!"<<endl; 
    return (void *)NULL;
}

TEST_F(DISABLED_RobustTest, Test_bigkey_hash_del) {
#define HsetBigKey(num) for(int n = 0; n < num; n++)\
    {\
        client->hset(key, field+itoa(n), val+itoa(n));\
    }\
    client->del(key);

    key = "hash_key";
    field = "field";
    val = "val";
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

TEST_F(DISABLED_RobustTest, Test_mset_mget_mthreads) {
    key = "key";
    val = "val";
    keysNum = 10000;
    keys.clear();
    kvs.clear();
    void * status;
    int setThreads = 1;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(key+itoa(n));
        kvs.insert(std::make_pair(key+itoa(n), val+itoa(n)));
    }

    s = client->multi_del(keys, &ret);
    if(!s.ok())
        cout<<"del fail!"<<s.code()<<endl;

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &mset_thread_func, this);
        usleep(100*1000);
    }

    for(int n = setThreads; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &mget_thread_func, this);
        usleep(100*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
        if(LDEBUG)
            cout<<"thread["<<n<<"] return status:"<<status<<endl;
    }

    list.clear();
    s = client->multi_get(keys, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->get(key+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<key+itoa(n)<<endl;
    }
    s = client->multi_del(keys);
    ASSERT_EQ(keysNum, list.size()/2)<<"Set fail at last:"<<list.size()/2<<endl;
}

TEST_F(DISABLED_RobustTest, Test_del_mset_mget_repeat) {
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

TEST_F(DISABLED_RobustTest, Test_multi_bigkey_del) {
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


TEST_F(DISABLED_RobustTest, Test_zset_del_zclear_block) {
//Issue:For zset key, del and then zclear will block forever.
    key = "key";
    field = "field";
    score = 1.0;
    client->zset(key, field, score);

    client->del(key);
    client->zclear(key, &ret);
    ASSERT_EQ(ret, 0)<<"fail to zclear key!"<<key<<endl;
}

TEST_F(DISABLED_RobustTest, Test_zset_del_set_rank_repeat) {
//Issue:For repeat del and zset, same members can be set.
//./integ-test --gtest_filter=*rank_repeat  --gtest_shuffle --gtest_break_on_failure --gtest_repeat=20 --gtest_also_run_disabled_tests
    key = "key";
    field = "field";
    score = GetRandomDouble_();
    client->del(key);
    for(int repeat = 0; repeat < 10000; repeat++)
    {
        client->del(key);
        int count = 2;

        for(int n = 0; n < count; n++)
        {
            client->zset(key, field + itoa(n), score + n);
        }

        for(int n = 0; n < count; n++)
        {
            s = client->zrrank(key, field + itoa(n), &ret);
            ASSERT_EQ(count-n-1, ret);
        }
        client->del(key);
    }
}
