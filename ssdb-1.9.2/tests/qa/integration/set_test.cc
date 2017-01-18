#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
#include <algorithm>
using namespace std;

class SetTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list, getList, comList;
        string key, field, val, getVal;
        uint16_t keysNum, totalNum, addNum;
        int64_t ret;
};

TEST_F(SetTest, Test_set_sadd) {
#define OKSadd(totalNum, addNum, list_str) s = client->sadd(key, list_str, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to sadd key!"<<endl;\
    ASSERT_EQ(addNum, ret)<<"Not add correct num elements:"<<key<<endl;\
    s = client->scard(key, &ret);\
    ASSERT_EQ(totalNum, ret)<<"Set size is not correct:"<<key<<endl;

#define FalseSadd s = client->sadd(key, "sadd", &ret);\
    ASSERT_TRUE(s.error())<<"this key should sadd fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_();
        client->del(key);
        //Add this for same keys should not add duplicated.
        list.clear();
        list.push_back(val);
        list.push_back(val);
        list.push_back(val+"0");
        OKSadd(2, 2, list)

        client->del(key);
        list.clear();
        OKSadd(1, 1, val)
        OKSadd(2, 1, val+"0")
        OKSadd(2, 0, val)
        list.clear();
        list.push_back(val+"0");
        list.push_back(val+"1");
        list.push_back(val+"2");
        OKSadd(4, 2, list)
        client->del(key);
    }

    // Some random keys
    keysNum = 100;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    list.clear(); 
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
        OKSadd(n+1, 1, list)
    }
    client->del(key);

    key = GetRandomKey_(); 
    val = GetRandomVal_();
    list.clear();
    comList.clear();
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
    }
    OKSadd(keysNum, keysNum, list)
    list.clear();
    for(int n = keysNum/2; n < keysNum*2; n++)
    {
        list.push_back(val+itoa(n)); 
    }
    OKSadd(keysNum*2, keysNum, list)
    client->del(key);

    //other types key
    field = GetRandomField_();
    val = GetRandomVal_();

    client->del(key);
    s = client->set(key, val);
    FalseSadd

    client->del(key); 
    s = client->hset(key, field, val);
    FalseSadd

    client->del(key); 
    client->qpush_front(key, val);
    FalseSadd

    client->del(key); 
    client->zset(key, field, 1.0);
    FalseSadd
    client->del(key);
}

TEST_F(SetTest, Test_set_srem) {
#define OKSrem(restNum, delNum, list_str) s = client->srem(key, list_str, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to srem key!"<<endl;\
    ASSERT_EQ(delNum, ret)<<"Not remove correct num elements:"<<key<<endl;\
    s = client->scard(key, &ret);\
    ASSERT_EQ(restNum, ret)<<"Rest set size is not correct:"<<key<<endl;

#define FalseSrem s = client->srem(key, "srem", &ret);\
    ASSERT_TRUE(s.error())<<"this key should srem fail!"<<endl;

    keysNum = 30;
    list.clear();
    comList.clear();
    val = GetRandomVal_();
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
    }
    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        client->del(key);
        client->sadd(key, list);
        OKSrem(keysNum-1, 1, val+"0")
        OKSrem(keysNum-1, 0, val+"0")
        OKSrem(keysNum-1, 0, val+"no")
        comList.clear();
        comList.push_back(val+"1");
        comList.push_back(val+"2");
        OKSrem(keysNum-3, 2, comList)
        comList.push_back(val+"3");
        comList.push_back(val+"4");
        OKSrem(keysNum-5, 2, comList)
        OKSrem(0, keysNum-5, list)
        client->del(key);
    }

    //Remove no exist key
    keysNum = 100;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    client->del(key);
    OKSrem(0, 0, val)

    //other types key
    s = client->del(key);
    s = client->set(key, val);
    FalseSrem
    s = client->del(key);
}

TEST_F(SetTest, Test_set_scard) {
    //other types key
    key = GetRandomKey_();
    val = GetRandomVal_();
    s = client->del(key);
    s = client->set(key, val);
    s = client->scard(key, &ret);
    ASSERT_TRUE(s.error())<<"this key should scard fail!"<<endl;
    s = client->del(key);
}

TEST_F(SetTest, Test_set_sismember) {

#define Sismember(TF, val) s = client->sismember(key, val, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to sismember key!"<<endl;\
    ASSERT_EQ(TF, ret)<<":"<<key<<endl;

    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_();
        client->del(key);
        Sismember(0, val)
        client->sadd(key, val);
        Sismember(1, val)
        Sismember(0, val+"null")
        client->srem(key, val);
    }

    // Some random keys
    keysNum = 100;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    list.clear(); 
    for(int n = 0; n < keysNum; n++) {
        client->sadd(key, val+itoa(n));
    }

    for(int n = 0; n < keysNum; n++) {
        Sismember(1, val+itoa(n))
    }
    Sismember(0, val+itoa(keysNum))
    client->del(key);
}

TEST_F(SetTest, Test_set_smembers) {

    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_();
        s = client->del(key);

        client->sadd(key, val);
        getList.clear();
        s = client->smembers(key, &getList);
        ASSERT_TRUE(s.ok());
        list.clear();
        list.push_back(val);
        ASSERT_EQ(list, getList)<<"set members fail"<<*it<<endl;
        client->srem(key, val);
    } 

    // Some random keys
    keysNum = 10;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    list.clear(); 
    for(int n = 0; n < keysNum; n++) {
        client->sadd(key, val+itoa(n));
        list.push_back(val+itoa(n));
    }
    getList.clear();
    s = client->smembers(key, &getList);
    sort(getList.begin(), getList.end());
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(list, getList)<<"set members fail"<<endl;

    client->del(key);

    getList.clear();
    s = client->smembers(key, &getList);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(0, getList.size());

    client->zset(key, val, 1.0);
    s = client->smembers(key, &getList);
    ASSERT_TRUE(s.error());

    client->del(key);
}

TEST_F(SetTest, Test_set_sunion_sunionstore) {
    std::vector<std::string> v(500), v2(500), set1, set2, set3, set4, set5;
    std::vector<std::string>::iterator it;
    key = GetRandomKey_();
    for (int i = 0; i < 200; i++){
        client->sadd("set1", itoa(i), &ret);
        set1.push_back(itoa(i));
        client->sadd("set2", itoa(i+195), &ret);                                              
        set2.push_back(itoa(i+195));

    }
    for (auto i : {199, 195, 1000, 2000}){
        client->sadd("set3", itoa(i), &ret);
        set3.push_back(itoa(i));
    }
    for (int i = 5; i < 200; i++){
        client->sadd("set4", itoa(i), &ret);
        set4.push_back(itoa(i));

    } 
    client->sadd("set5", itoa(0), &ret);
    set5.push_back(itoa(0));

    list.clear();
    list.push_back("set1");
    list.push_back("set2");

    std::sort(set1.begin(), set1.end());
    std::sort(set2.begin(), set2.end());
    it = std::set_union(set1.begin(),set1.end(),set2.begin(),set2.end(),v.begin());
    v.resize(it-v.begin());
    std::sort(v.begin(), v.end());

    getList.clear();
    s = client->sunion(list, &getList);
    ASSERT_TRUE(s.ok())<<"sunion should return ok!"<<s.code();
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v.begin(),v.end(),getList.begin()))<<"sunion with two sets"<<endl;

    client->del(key);
    s = client->sunionstore(key, list, &ret);
    ASSERT_TRUE(s.ok())<<"sunionstore should return ok!"<<s.code();
    getList.clear();
    client->smembers(key, &getList);
    EXPECT_EQ(ret, getList.size())<<"sunionstore should return the union elements numbers.";
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v.begin(),v.end(),getList.begin()))<<"sunionstore with two sets"<<endl;

    client->del("key");
    list.push_back("key");

    getList.clear();
    s = client->sunion(list, &getList);
    ASSERT_TRUE(s.ok())<<"sunion with non-exist key should return ok!";
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v.begin(),v.end(),getList.begin()))<<"sunion with non-exist key"<<endl;

    s = client->sunionstore(key, list, &ret);
    ASSERT_TRUE(s.ok())<<"sunionstore with non-exist key should return ok!";
    getList.clear();
    client->smembers(key, &getList);
    EXPECT_EQ(ret, getList.size())<<"sunionstore swith non-exist key hould return the union elements numbers.";
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v.begin(),v.end(),getList.begin()))<<"sunionstore with two sets"<<endl;


    list.push_back("set3");
    std::sort(set3.begin(), set3.end());
    it = std::set_union(v.begin(),v.end(),set3.begin(),set3.end(),v2.begin());
    v2.resize(it-v2.begin());
    std::sort(v2.begin(), v2.end());

    getList.clear();
    s = client->sunion(list, &getList);
    ASSERT_TRUE(s.ok())<<"sunion should return ok!";
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v2.begin(),v2.end(),getList.begin()))<<"sunion with multi sets"<<endl;

    client->sadd(key, "override");

    s = client->sunionstore(key, list, &ret);
    ASSERT_TRUE(s.ok())<<"sunionstore should return ok!";
    getList.clear();
    client->smembers(key, &getList);
    EXPECT_EQ(ret, getList.size())<<"sunionstore should return the union elements numbers.";
    std::sort(getList.begin(), getList.end());
    EXPECT_TRUE(std::equal(v2.begin(),v2.end(),getList.begin()))<<"sunionstore with multi sets"<<endl;

    client->set("key", "val");

    getList.clear();
    s = client->sunion(list, &getList);
    ASSERT_TRUE(s.error())<<"sunion with non-set should return error!";
    ASSERT_TRUE(getList.empty());

    getList.clear();
    s = client->sunionstore(key, list, &ret);
    ASSERT_TRUE(s.error())<<"sunionstore with non-set should return error!";
    ASSERT_TRUE(getList.empty());

    list.clear();
    list.push_back("non-exist key");
    client->sunionstore(key, list, &ret);
    s = client->get(key, &getVal);
    EXPECT_TRUE(s.not_found())<<"sunionstore with non-exist key should del key:"<<s.code()<<endl;

    client->del("key");
    for (int i = 1; i <= 5; i++){
        client->del("set"+itoa(i));
    }

    client->del(key);
    for (int i = 1; i <= 5; i++){
        client->del("set"+itoa(i));
    }

}
