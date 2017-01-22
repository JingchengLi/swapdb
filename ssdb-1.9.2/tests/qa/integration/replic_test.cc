//
// Created by a1 on 17-1-13.
//
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

class ReplicTest : public SSDBTest
{
public:
    ssdb::Status s;
    string key, val, getVal, field;
    std::map<std::string, std::string> kvs;
    std::vector<std::string> keys, getList;
    uint32_t keysNum;
    int64_t ret;
    double score, getScore;

    virtual void SetUp(){
        port = 8888;
        ip = "127.0.0.1";
        client = ssdb::Client::connect(ip.data(), port);
        ASSERT_TRUE(client != NULL)<<"fail to connect to server!";

        slave_port = 8889;
        slave_ip = "127.0.0.1";
        slave_client = ssdb::Client::connect(slave_ip.data(), slave_port);
        ASSERT_TRUE(slave_client != NULL)<<"fail to connect to slave_server!";
    }

    virtual void TearDown(){
        delete client;
        delete slave_client;
    }

protected:
    ssdb::Client *slave_client;
    string slave_ip;
    int slave_port;
};

TEST_F(ReplicTest, Test_replic_types){
    keysNum = 100;
    val = "val";
    field = "field";
    score = 0;
    uint16_t counts = 10;

    keys.clear();
    kvs.clear();
    for (int n = 0; n < counts; ++n) {
        //string type
        key = "kkey_"+itoa(n);
        kvs.clear();
        for (int m = 0; m < keysNum; ++m) {
            kvs.insert(make_pair(key+itoa(m), val+itoa(m)));
            keys.push_back(key+itoa(m));
            client->multi_set(kvs);
        }

        //hash type
        kvs.clear();
        key = "hkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            kvs.insert(make_pair(field+itoa(m), val+itoa(m)));
        }
        client->multi_hset(key, kvs);
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

    s = client->replic(slave_ip, slave_port);
    // ASSERT_TRUE(s.ok())<<"replic fail!"<<s.code()<<endl;
    
    do {
        sleep(1);
        s = slave_client->get("kkey_00", &getVal);
    } while (!s.ok());

    for (int n = 0; n < counts; ++n) {
        //string type
        key = "kkey_"+itoa(n);
        for (int m = 0; m < keysNum; ++m) {
            slave_client->get(key+itoa(m), &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<m;
        }

        //hash type
        key = "hkey_"+itoa(n);
        slave_client->hsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave hsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            slave_client->hget(key, field+itoa(m), &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<m;
        }

        //list type
        key = "lkey_"+itoa(n);
        slave_client->qsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave qsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            slave_client->qget(key, m, &getVal);
            ASSERT_EQ(val+itoa(m), getVal)<<m;
        }

        //set type
        key = "skey_"+itoa(n);
        slave_client->scard(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave scard error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            slave_client->sismember(key, val+itoa(m), &ret);
            ASSERT_EQ(true, ret)<<m;
        }

        //zset type
        key = "zkey_"+itoa(n);
        slave_client->zsize(key, &ret);
        ASSERT_EQ(keysNum, ret)<<"slave zsize error"<<endl;

        for (int m = 0; m < keysNum; ++m) {
            slave_client->zget(key, field+itoa(m), &getScore);
            ASSERT_EQ(m, getScore)<<m;
        }
    }
    slave_client->multi_del(keys);
    client->multi_del(keys);
}

TEST_F(ReplicTest, Test_replic_lens){
    // std::vector<int64_t> lens = {0, 1<<6-1, 1<<6, 1<<14-1, 1<<14, 1<<32-1, 1<<32};
    std::vector<int64_t> lens = {0, (1<<6)-1, 1<<6, 1000, (1<<14)-1, 1<<14};
    key = "key";
    for (auto l : lens) {
        val.clear();
        val.append(l, 'a');
        client->set(key, val);

        s = client->replic(slave_ip, slave_port);
        // ASSERT_TRUE(s.ok())<<"replic fail!"<<s.code()<<l<<endl;

        do {
            sleep(1);
            s = slave_client->get(key, &getVal);
        } while (!s.ok());

        EXPECT_EQ(l, getVal.size());
        slave_client->multi_del(key);
        client->multi_del(key);
    }
}

TEST_F(ReplicTest, Test_replic_expire_keys){
}

TEST_F(ReplicTest, Test_replic_multi_slaves){
}
