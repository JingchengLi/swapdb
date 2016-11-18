#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

class ZsetTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list;
        std::map<std::string, std::string> kvs;
        std::vector<std::string> keys;
        string key, field;
        double score, getScore;
        uint16_t keysNum;
        int64_t ret;
};

TEST_F(ZsetTest, Test_zset_zset) {
#define OKZset  s = client->zset(key, field, score);\
    ASSERT_TRUE(s.ok())<<"fail to zset key!"<<key<<field<<endl;\
    s = client->zget(key, field, &getScore);\
    ASSERT_TRUE(s.ok())<<"fail to zget key score!"<<endl;\
    ASSERT_NEAR(score, getScore, eps);

#define FalseZset s = client->zset(key, field, score);\
    ASSERT_TRUE(s.error())<<"this key should set fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        score = GetRandomDouble_(); 
        client->del(key);
        OKZset

        // set exsit key
        field = GetRandomField_();
        score = GetRandomDouble_(); 
        OKZset
        s = client->zdel(key, field); 
    } 

    // Some random keys
    keysNum = 100;
    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = GetRandomDouble_(); 
    s = client->zclear(key);
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        score += 1; 
        OKZset
    }
    s = client->zsize(key, &ret);
    ASSERT_EQ(keysNum, ret);
    s = client->zclear(key);

    // other types key
    s = client->del(key);
    field = GetRandomField_();
    s = client->set(key, "score");
    FalseZset
    s = client->del(key); 
}

TEST_F(ZsetTest, Test_zset_zget) {
#define NotFoundZget s = client->zget(key, field, &getScore);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;

    // Some random keys
    for(int n = 0; n < 5; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        score = GetRandomDouble_(); 
        s = client->del(key);
        NotFoundZget
    }

    keysNum = 10;
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        s = client->zset(key, field, score);
    }
    field = field+itoa(keysNum);
    NotFoundZget
    s = client->zclear(key); 
}

TEST_F(ZsetTest, Test_zset_zdel) {
#define OKZdel s = client->zdel(key, field);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    s = client->zget(key, field, &getScore);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define NotFoundHdel s = client->zdel(key, field);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        score = GetRandomDouble_(); 
        field = GetRandomField_();
        s = client->zset(key, field, score);
        OKZdel
        NotFoundHdel
    }

    keysNum = 100;
    score = GetRandomDouble_(); 
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        s = client->zset(key, field, score);
        OKZdel
        NotFoundHdel
    }
}

TEST_F(ZsetTest, Test_zset_zincr) {
#define OKZincr incr = GetRandomDouble_();\
    s = client->del(key);\
    s = client->zincr(key, field, incr, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->zget(key, field, &getScore);\
    ASSERT_NEAR(incr, getScore, eps);\
    \
    s = client->zincr(key, field, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->zget(key, field, &getScore);\
    ASSERT_NEAR(incr+n, getScore, eps);\
    s = client->zdel(key, field);

    double incr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        field = GetRandomField_();
        OKZincr
    }

    //Some random keys
    keysNum = 10;
    score = GetRandomDouble_(); 
    for(n = 0; n < keysNum; n++)
    {
        key = GetRandomKey_(); 
        field = GetRandomField_();
        OKZincr
    }

    s = client->del(key);
    s = client->zincr(key, field, DBL_MAX, &ret);
    s = client->zincr(key, field, 1, &ret);
    ASSERT_TRUE(s.error())<<"Exceed DBL_MAX!";
    s = client->zget(key, field, &getScore);
    ASSERT_NEAR(DBL_MAX, getScore, eps);

    s = client->del(key);
    s = client->zincr(key, field, -DBL_MAX, &ret);
    s = client->zincr(key, field, -1, &ret);
    ASSERT_TRUE(s.error())<<"Exceed -DBL_MAX!";
    s = client->zget(key, field, &getScore);
    ASSERT_NEAR(-DBL_MAX, getScore, eps);
    s = client->zdel(key, field);
}

TEST_F(ZsetTest, Test_zset_zsize) {
#define OKZsize(num) s = client->zsize(key, &ret);\
    ASSERT_EQ(ret, num)<<"fail to zsize key!"<<key<<endl;\
    ASSERT_TRUE(s.ok())<<"hsize key not ok!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        score = GetRandomDouble_(); 
        s = client->del(key);
        OKZsize(0)
        s = client->zset(key, field, score);
        OKZsize(1)
        s = client->zdel(key, field);
    }

    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = GetRandomDouble_(); 
    s = client->del(key);
    for(int n = 0; n < 10; n++)
    {
        field = field+itoa(n);
        score = score + n;
        client->zset(key, field, score);
        OKZsize(n+1)
    }
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_zclear) {
#define OKZclear(num) s = client->zclear(key, &ret);\
    ASSERT_EQ(ret, num)<<"fail to zclear key!"<<key<<endl;\
    s = client->zsize(key, &ret);\
    ASSERT_EQ(ret, 0)<<"key is not null!"<<key<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        field = GetRandomField_();
        score = GetRandomDouble_(); 
        s = client->del(key);
        OKZclear(0)
        s = client->zset(key, field, score);
        OKZclear(1)
    }

    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = GetRandomDouble_(); 
    s = client->del(key);
    for(int n = 0; n < 10; n++)
    {
        field = field+itoa(n);
        score = score+n;
        client->zset(key, field, score);
    }
    OKZclear(10)
}

TEST_F(ZsetTest, Test_zset_zkeys) {
    key = GetRandomKey_();
    client->del(key);
    score = 2.0;
    s = client->zkeys(key, "", NULL, &score, 5, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);
    list.clear();
    client->zset(key, "field1", -1);
    client->zset(key, "field2", 2);
    client->zset(key, "field3", 3);
    s = client->zkeys(key, "", NULL, &score, 5, &list);
    ASSERT_TRUE(s.ok() && list.size() == 2);
    ASSERT_EQ("field1", list[0]);
    ASSERT_EQ("field2", list[1]);
    score = 3.0;
    list.clear();
    s = client->zkeys(key, "", NULL, &score, 5, &list);
    ASSERT_TRUE(s.ok() && list.size() == 3);
    ASSERT_EQ("field3", list[2]);
    list.clear();
    s = client->zkeys(key, "", NULL, &score, 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 2);
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_zrange) {
    key = GetRandomKey_();
    field = GetRandomField_();
    s = client->del(key);
    s = client->zrange(key, 1, 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);
    list.clear();
    client->zset(key, field + '1', -2.4);
    client->zset(key, field + '0', -2.5);
    client->zset(key, field + '2', 0);
    client->zset(key, field + '3', 1);
    s = client->zrange(key, 1, 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ(field + '1', list[0]);
    ASSERT_NEAR(-2.4, atof(list[1].data()), eps);
    ASSERT_EQ(field + '2', list[2]);
    ASSERT_NEAR(0, atof(list[3].data()), eps);
    list.clear();
    s = client->zrange(key, 0, 3, &list);
    ASSERT_EQ(6, list.size());
    ASSERT_EQ(field + '0', list[0]);
    ASSERT_NEAR(-2.5, atof(list[1].data()), eps);
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_zrrange) {
    key = "key";//GetRandomKey_();
    field = "field";//GetRandomField_();
    s = client->del(key);
    s = client->zrrange(key, 1, 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);
    list.clear();
    client->zset(key, field + '1', -2.4);
    client->zset(key, field + '0', -2.5);
    client->zset(key, field + '2', 0);
    client->zset(key, field + '3', 1);
    s = client->zrrange(key, 1, 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ(field + '2', list[0]);
    ASSERT_NEAR(0, atof(list[1].data()), eps);
    ASSERT_EQ(field + '1', list[2]);
    ASSERT_NEAR(-2.4, atof(list[3].data()), eps);
    list.clear();
    s = client->zrrange(key, 0, 3, &list);
    ASSERT_EQ(6, list.size());
    ASSERT_EQ(field + '3', list[0]);
    ASSERT_NEAR(1, atof(list[1].data()), eps);
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_zrank) {
    key = GetRandomKey_();
    field = GetRandomField_();
    score = GetRandomDouble_();
    s = client->del(key);
    int count = 20;

    for(int n = 0; n < count; n++)
    {
        client->zset(key, field + itoa(n), score + n);
    }

    for(int n = 0; n < count; n++)
    {
        s = client->zrank(key, field + itoa(n), &ret);
        ASSERT_EQ(n, ret);
    }
    client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_zrrank) {
    key = "key";//GetRandomKey_();
    field = "field";//GetRandomField_();
    score = GetRandomDouble_();
    s = client->del(key);
    int count = 20;

    for(int n = 0; n < count; n++)
    {
        client->zset(key, field + itoa(n), score + n);
    }

    for(int n = 0; n < count; n++)
    {
        s = client->zrrank(key, field + itoa(n), &ret);
        ASSERT_EQ(count-n-1, ret);
    }
    client->zclear(key);
}

/* 
TEST_F(ZsetTest, Test_zset_hscan) {
    key = GetRandomKey_();
    s = client->zscan(key, "", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->del("key");
    client->zset("key", "00000000f1","v1");
    client->zset("key", "00000000f2","v2");
    s = client->zscan("key", "00000000f0", "00000000f2", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000f1", list[0]);
    ASSERT_EQ("v1", list[1]);
    ASSERT_EQ("00000000f2", list[2]);
    ASSERT_EQ("v2", list[3]);
    list.clear();
    s = client->zscan("key", "00000000f2", "00000000f0", 2, &list);
    ASSERT_EQ(0, list.size());
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_hrscan) {
    key = GetRandomKey_();
    s = client->zrscan(key, "", "", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() <= 4);
    list.clear();
    client->del("key");
    client->zset("key", "00000000f1","v1");
    client->zset("key", "00000000f2","v2");
    s = client->zrscan("key", "00000000f3", "00000000f1", 2, &list);
    ASSERT_TRUE(s.ok() && list.size() == 4);
    ASSERT_EQ("00000000f2", list[0]);
    ASSERT_EQ("v2", list[1]);
    ASSERT_EQ("00000000f1", list[2]);
    ASSERT_EQ("v1", list[3]);
    list.clear();
    s = client->zrscan("key", "00000000f1", "00000000f3", 2, &list);
    ASSERT_EQ(0, list.size());
    s = client->zclear(key);
}

TEST_F(ZsetTest, Test_zset_multi_hset_hget_hdel) {
    string key, field1, field2, field3, score1, score2, score3;

    key = GetRandomKey_(); 
    field1 = GetRandomField_();
    field2 = field1+'2';
    field3 = field1+'3';
    score1 = GetRandomDouble_();
    score2 = score1+'2';
    score3 = score1+'3';
    kvs.clear();
    keys.clear();
    list.clear();
    kvs.insert(std::make_pair(field1, score1));
    kvs.insert(std::make_pair(field2, score2));
    keys.push_back(field1);
    keys.push_back(field2);
    keys.push_back(field3);

    //all keys not exist
    s = client->multi_hdel(key, keys);
    ASSERT_TRUE(s.ok());
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(0, list.size());
    s = client->multi_hset(key, kvs);
    ASSERT_TRUE(s.ok());
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(4, list.size());
    ASSERT_EQ(field1, list[0]);
    ASSERT_EQ(score1, list[1]);
    ASSERT_EQ(field2, list[2]);
    ASSERT_EQ(score2, list[3]);
    kvs.insert(std::make_pair(field3, score3));

    //one key not exist, two keys exist
    s = client->multi_hset(key, kvs);
    ASSERT_TRUE(s.ok());
    list.clear();
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(6, list.size());
    kvs.clear();
    score1 = score1+'1';
    score2 = score2+'2';
    score3 = score3+'3';
    kvs.insert(std::make_pair(field1, score1));
    kvs.insert(std::make_pair(field2, score2));
    kvs.insert(std::make_pair(field3, score3));

    //all keys exist, update their scores
    s = client->multi_hset(key, kvs);
    ASSERT_TRUE(s.ok());
    list.clear();
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(6, list.size());
    ASSERT_EQ(field1, list[0]);
    ASSERT_EQ(score1, list[1]);
    ASSERT_EQ(field2, list[2]);
    ASSERT_EQ(score2, list[3]);
    ASSERT_EQ(field3, list[4]);
    ASSERT_EQ(score3, list[5]);
    s = client->multi_hdel(key, keys);
    ASSERT_TRUE(s.ok());
    list.clear();
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(0, list.size());
    kvs.clear();
    list.clear();
    keys.clear();

    int fieldNum = 10;
    for(int n = 0; n < fieldNum; n++)
    {
        kvs.insert(std::make_pair(field1 + itoa(n), score1 + itoa(n)));
        keys.push_back(field1 + itoa(n));
    }
    s = client->multi_hset(key, kvs);
    ASSERT_TRUE(s.ok());
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(fieldNum*2, list.size());

    for(int n = 0; n < fieldNum; n++)
    {
        ASSERT_EQ(field1 + itoa(n), list[n*2]);
        ASSERT_EQ(score1 + itoa(n), list[n*2+1]);
    }

    s = client->multi_hdel(key, keys);
    ASSERT_TRUE(s.ok());
    list.clear();
    s = client->multi_hget(key, keys, &list);
    ASSERT_EQ(0, list.size());
}
TEST_F(ZsetTest, Test_zset_hgetall) {
#define NotFoundHgetall s = client->zgetall(key, &list);\
    EXPECT_TRUE(s.not_found())<<"this key should be not found!"<<endl;\
    ASSERT_EQ(0, list.size());

    key = GetRandomKey_(); 
    score = GetRandomDouble_(); 
    field = GetRandomField_();
    s = client->zclear(key);
    NotFoundHgetall
    keysNum = 10;
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        score = score+itoa(n);
		kvs.insert(std::make_pair(field, score));
        s = client->zset(key, field, score);
    }
    s = client->zgetall(key, &list);
    for(int n = 0; n < keysNum; n += 2)
    {
        EXPECT_EQ(kvs[list[n]], list[n+1]);
    }
    s = client->zclear(key);
}

*/
