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
        std::map<std::string, double> items;
        std::vector<std::string> keys;
        string key, field, val;
        double score, getScore;
        uint16_t keysNum;
        int64_t ret;
        //Currently support range
        const int64_t ZSET_SCORE_MAX = 10000000000000LL;               //10,000,000,000,000 ( *10,000 for Shift  )
        const int64_t ZSET_SCORE_MIN = -ZSET_SCORE_MAX;

};

TEST_F(ZsetTest, Test_zset_zadd) {
#define OKZset  s = client->zset(key, field, score);\
    ASSERT_TRUE(s.ok())<<"fail to zset key!"<<key<<s.code()<<endl;\
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

        score = GetRandomDouble_(); 
//same key field set cannot add, just change score.
        OKZset
        client->zsize(key, &ret);
        ASSERT_EQ(2, ret)<<"fail to zsize key!"<<key<<endl;

        s = client->zdel(key, field); 
        client->del(key);
    } 

    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = ZSET_SCORE_MAX;
    
    OKZset

    score = ZSET_SCORE_MIN;
    
    OKZset

    score = ZSET_SCORE_MAX+1;

    FalseZset

    score = ZSET_SCORE_MIN-1;

    FalseZset

    // Some random keys
    keysNum = 100;
    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = GetRandomDouble_(); 
    s = client->del(key);
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        score += 1; 
        OKZset
    }
    s = client->zsize(key, &ret);
    ASSERT_EQ(keysNum, ret);
    s = client->del(key);


    // other types key
    field = GetRandomField_();
    val = GetRandomVal_();

    client->del(key);
    s = client->set(key, val);
    FalseZset

    client->del(key); 
    s = client->hset(key, field, val);
    FalseZset

    client->del(key); 
    client->qpush_front(key, val);
    FalseZset

    client->del(key); 
    client->sadd(key, val);
    FalseZset
}

TEST_F(ZsetTest, Test_zset_zscore) {
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
    s = client->del(key); 
}

TEST_F(ZsetTest, Test_zset_zrem) {
#define OKZdel s = client->zdel(key, field, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to delete key!"<<endl;\
    ASSERT_EQ(1, ret)<<"should delete one"<<endl;\
    s = client->zget(key, field, &getScore);\
    ASSERT_TRUE(s.not_found())<<"this key should be deleted!"<<endl;

#define FalseZdel s = client->zdel(key, field);\
    ASSERT_TRUE(s.error())<<"this key should zdel fail!"<<endl;

#define NotFoundZdel s = client->zdel(key, field, &ret);\
    ASSERT_EQ(0, ret)<<"should delete null"<<endl;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        score = GetRandomDouble_(); 
        field = GetRandomField_();
        s = client->zset(key, field, score);
        OKZdel
    }

    keysNum = 100;
    score = GetRandomDouble_(); 
    for(int n = 0; n < keysNum; n++)
    {
        field = field+itoa(n);
        s = client->zset(key, field, score);
        OKZdel
    }
    
    client->del(key);
    NotFoundZdel
    client->sadd(key, "val");
    FalseZdel
    client->del(key);
}

TEST_F(ZsetTest, Test_zset_zincrby) {
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

#define FalseZincr s = client->zincr(key, field, 1, &ret);\
    ASSERT_TRUE(s.error())<<"this key should zincr fail!"<<endl;

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

    // other types key
    field = GetRandomField_();
    val = GetRandomVal_();

    client->del(key);
    s = client->set(key, val);
    FalseZincr

    client->del(key); 
    s = client->hset(key, field, val);
    FalseZincr

    client->del(key); 
    client->qpush_front(key, val);
    FalseZincr

    client->del(key); 
    client->sadd(key, val);
    FalseZincr

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

TEST_F(ZsetTest, Test_zset_zdecrby) {
#define OKZdecr decr = GetRandomDouble_();\
    s = client->del(key);\
    s = client->zdecr(key, field, decr, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->zget(key, field, &getScore);\
    ASSERT_NEAR(-1*decr, getScore, eps);\
    \
    s = client->zdecr(key, field, n, &ret);\
    ASSERT_TRUE(s.ok());\
    s = client->zget(key, field, &getScore);\
    ASSERT_NEAR(-1*(decr+n), getScore, eps);\
    s = client->zdel(key, field);

    double decr, ret, n = 0;
    //Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        n++;
        key = *it;
        field = GetRandomField_();
        OKZdecr
    }
}

TEST_F(ZsetTest, Test_zset_zcard) {
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
    s = client->del(key);
}

TEST_F(ZsetTest, Test_zset_zclear) {
//not use this command, use del instead
#define OKZclear(num) s = client->zclear(key, &ret);\
    ASSERT_EQ(num, ret)<<"fail to zclear key!"<<key<<endl;\
    s = client->zsize(key, &ret);\
    ASSERT_EQ(0, ret)<<"key is not null!"<<key<<endl;

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

TEST_F(ZsetTest,  Test_zset_zkeys) {
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
    ASSERT_EQ("ok", s.code());
    ASSERT_EQ(2, list.size());
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
    s = client->del(key);
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
    s = client->del(key);
    list.clear();
    s = client->zrange(key, 0, 3, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);

    //del should clear zset types
    client->zset(key, field + '1', -2.4);
    client->zset(key, field + '0', -2.5);
    client->zset(key, field + '2', 0);
    client->zset(key, field + '3', 1);
    s = client->del(key);
    list.clear();
    s = client->zrange(key, 0, 3, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);
}

TEST_F(ZsetTest, Test_zset_zrevrange) {
    key = GetRandomKey_();
    field = GetRandomField_();
    s = client->del(key);
    s = client->zrrange(key, 1, 2, &list);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(0, list.size());
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
    s = client->del(key);
    list.clear();
    s = client->zrrange(key, 0, 3, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);

    //del should clear zset types
    client->zset(key, field + '1', -2.4);
    client->zset(key, field + '0', -2.5);
    client->zset(key, field + '2', 0);
    client->zset(key, field + '3', 1);
    s = client->del(key);
    list.clear();
    s = client->zrrange(key, 0, 3, &list);
    ASSERT_TRUE(s.ok() && list.size() == 0);
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
    client->del(key);

    client->set(key, "val");
    s = client->zrank(key, field, &ret);
    ASSERT_TRUE(s.error());
    client->del(key);
}

TEST_F(ZsetTest, Test_zset_zrevrank) {
    key = GetRandomKey_();
    field = GetRandomField_();
    score = GetRandomDouble_();
    client->del(key);
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
    client->del(key);
    client->set(key, "val");
    s = client->zrrank(key, field, &ret);
    ASSERT_TRUE(s.error());
    client->del(key);
}

TEST_F(ZsetTest, Test_zset_multi_zadd_zdel) {

    //multi fields zadd
    key = GetRandomKey_(); 
    field = GetRandomField_();
    score = GetRandomDouble_(); 
    items.clear();
    keys.clear();
    int counts = 10;
    for(int n = 0;n < counts; n++){
        items.insert(std::make_pair(field+itoa(n),score+n));
        keys.push_back(field+itoa(n));
    }
    s = client->zset(key, items, &ret);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(counts, ret);
    for(int n = 0;n < counts; n++){
        s = client->zget(key, field+itoa(n), &getScore);
        ASSERT_EQ("ok", s.code());
        ASSERT_NEAR(score+n, getScore, eps);
    }
    s = client->zset(key, items, &ret);
    ASSERT_EQ(0, ret);

    items.clear();
    keys.clear();
    for(int n = 0;n < counts; n+=2){
        items.insert(std::make_pair(field+itoa(n),score+n));
        keys.push_back(field+itoa(n));
    }
    s = client->zdel(key, keys, &ret);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(counts/2, ret);
    s = client->zsize(key, &ret);
    ASSERT_EQ(counts/2, ret);
    s = client->zdel(key, keys, &ret);
    ASSERT_EQ(0, ret);
}
