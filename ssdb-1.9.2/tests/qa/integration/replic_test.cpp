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
    std::string key, val;
    std::map<std::string, std::string> kvs;

};

TEST_F(ReplicTest, Test_replic_slave){
    for (int i = 0; i < 1000000; ++i) {
        val = itoa(i);
        key = "kkey_";
        key.append(val);
        kvs.insert(make_pair(key, val));
        s = client->set(key, val);
        ASSERT_TRUE(s.ok())<<"fail to set key!"<<key<<endl;
    }

    s = client->replic("127.0.0.1", 8889);
}

TEST_F(ReplicTest, Test_check_result){
    ssdb::Client *slave_client = ssdb::Client::connect("127.0.0.1", 8889);
    if (slave_client == NULL) {
        printf("fail to connect to slave server!\n");
        return;
    }
    for (int i = 0; i < 1000000; ++i) {
        val = itoa(i);
        key = "kkey_";
        key.append(val);
        std::string db_val;
        s = slave_client->get(key, &db_val);
        ASSERT_TRUE(s.ok()&&(val == db_val))<<"fail to get key val!"<<key<<endl;
    }

    delete slave_client;
}