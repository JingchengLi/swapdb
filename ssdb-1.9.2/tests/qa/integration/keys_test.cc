#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

#ifndef LONG_LONG_MAX
    #define LONG_LONG_MAX 0x7fffffffffffffffLL
#endif

class KeysTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list;
        std::vector<std::string> keys;
        std::map<std::string, std::string> kvs;
        string key, val, getVal, field;
        uint32_t keysNum;
        int64_t ret, score;
        int64_t ttl, pttl;
};

#define OKExpire s = client->expire(key, ttl, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to expire key!"<<endl;\
    ASSERT_EQ(1, ret);\
    s = client->ttl(key, &ret);\
    ASSERT_NEAR(ttl, ret, 1);

//when key does not exist, expire set false
#define FalseExpire s = client->expire(key, ttl, &ret);\
    ASSERT_EQ(0, ret);\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;\
    s = client->ttl(key, &ret);\
    EXPECT_EQ(-2, ret);

#define TTL(n) s = client->ttl(key, &ret);\
    EXPECT_EQ(n, ret)<<"ttl should return "<<n<<" when the key no expire time or no exist!";

#define NEAR_TTL(n) s = client->ttl(key, &ret);\
    ASSERT_NEAR(n, ret, 1)<<"ttl should near key expire time!";

TEST_F(KeysTest, Test_keys_expire) {

    key = GetRandomKey_(); 
    field = GetRandomField_();
    val = GetRandomVal_(); 
    ttl = 2;

    FalseExpire

//kv type
    client->set(key,val);
    TTL(-1)
    OKExpire

    sleep(ttl+1);
    FalseExpire

    ttl = 10;
    client->setx(key,val, 5);
    OKExpire

    client->getset(key, val, &getVal);
    TTL(-1)
    OKExpire

    client->del(key);
    TTL(-2)
    FalseExpire
    client->del(key);

    //hash
    ttl = 10;
    client->del(key);
    TTL(-2)
    client->hset(key, field, val);
    TTL(-1)
    OKExpire

    client->hset(key, field, val);
    NEAR_TTL(ttl)
    client->hset(key, field+itoa(0), val+itoa(0));
    NEAR_TTL(ttl)

    client->hdel(key, field+itoa(0));
    NEAR_TTL(ttl)
    client->hdel(key, field);
    TTL(-2)

    client->hset(key, field, val);
    OKExpire
    client->del(key);
    TTL(-2)

    //list
    ttl = 10;
    client->del(key);
    TTL(-2)
    client->qpush_front(key, val);
    TTL(-1)
    OKExpire

    client->qpush_back(key, val);
    NEAR_TTL(ttl)

    client->qpop_back(key, &getVal);
    NEAR_TTL(ttl)
    client->qpop_back(key, &getVal);
    TTL(-2)

    client->qpush_back(key, val);
    OKExpire
    client->del(key);
    TTL(-2)

    //set
    ttl = 10;
    client->del(key);
    TTL(-2)
    client->sadd(key, val);
    TTL(-1)
    OKExpire

    client->sadd(key, val+itoa(0));
    NEAR_TTL(ttl)

    client->srem(key, val+itoa(0));
    NEAR_TTL(ttl)
    client->srem(key, val);
    TTL(-2)

    client->sadd(key, val);
    OKExpire
    client->del(key);
    TTL(-2)

    //zset
    ttl = 10;
    client->del(key);
    TTL(-2)
    client->zset(key, field, 1.0);
    TTL(-1)
    OKExpire

    client->zset(key, field+itoa(0), 2.0);
    NEAR_TTL(ttl)

    client->zdel(key, field+itoa(0));
    NEAR_TTL(ttl)
    client->zdel(key, field);
    TTL(-2)

    client->zset(key, field, 1.0);
    OKExpire
    client->del(key);
    TTL(-2)

    for(int n = 0; n < 2; n++){
        ttl = n&0x1?LONG_LONG_MAX/1000:LONG_LONG_MAX;
        client->set(key, "val");
        client->expire(key, ttl, &ret);
        getVal.clear();
        client->get(key, &getVal);
        EXPECT_EQ("val", getVal)<<"expire ttl "<<ttl<<" should not del key"<<endl;
        sleep(1);
        getVal.clear();
        client->get(key, &getVal);
        EXPECT_EQ("val", getVal)<<"expire ttl "<<ttl<<" should not del key after 1 sec"<<endl;
        client->del(key);
    }
}

TEST_F(KeysTest, Test_keys_ttl) {
    key = GetRandomKey_(); 
    field = GetRandomField_();
    val = GetRandomVal_(); 
    score = GetRandomDouble_();
    ttl = 1;

//when ttl exceed, key was deleted.
    for(int n = 0; n < 5; n++){
        switch(n){
            ttl += 1;
            client->del(key);
            TTL(-2)
            case 0:
                client->set(key,val);
                break;
            case 1:
                client->hset(key, field, val);
                break;
            case 2:
                client->qpush_front(key, val);
                break;
            case 3:
                client->sadd(key, val);
                break;
            case 4:
                client->zset(key, field, score);
                break;
        }
        TTL(-1)
        client->expire(key, ttl, &ret);
        NEAR_TTL(ttl)
        sleep(ttl-1);
        s = client->get(key, &getVal);
        ASSERT_FALSE(s.not_found())<<"this key should be found!"<<endl;
        sleep(2);
        s = client->get(key, &getVal);
        ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;
        TTL(-2)
    } 

//Expire ttl <= 0, then key was deleted
    for(int n = 0; n < 5; n++){
        switch(n){
            ttl += 1;
            client->del(key);
            TTL(-2)
            case 0:
                client->set(key,val);
                break;
            case 1:
                client->hset(key, field, val);
                break;
            case 2:
                client->qpush_front(key, val);
                break;
            case 3:
                client->sadd(key, val);
                break;
            case 4:
                client->zset(key, field, score);
                break;
        }
        client->expire(key, -n, &ret);
        s = client->get(key, &getVal);

        ASSERT_TRUE(s.not_found())<<n<<"this key should be not found!"<<s.code()<<endl;
        TTL(-2)
    }
}

#define OKPexpire s = client->pexpire(key, pttl, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to pexpire key!"<<endl;\
    ASSERT_EQ(1, ret);\
    s = client->pttl(key, &ret);\
    ASSERT_TRUE((ret > pttl-2000) && ret <= pttl);

//when key does not exist, expire set false
#define FalsePexpire s = client->pexpire(key, pttl, &ret);\
    ASSERT_EQ(0, ret);\
    s = client->get(key, &getVal);\
    ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;\
    s = client->pttl(key, &ret);\
    EXPECT_EQ(-2, ret);

#define PTTL(n) s = client->pttl(key, &ret);\
    EXPECT_EQ(n, ret)<<"pttl should return "<<n<<" when the key no expire time or no exist!";

#define NEAR_PTTL(n) s = client->pttl(key, &ret);\
    ASSERT_TRUE((ret > n-2000) && ret <= n)<<"pttl should near key expire time!";

TEST_F(KeysTest, Test_keys_pexpire) {

    key = GetRandomKey_();
    field = GetRandomField_();
    val = GetRandomVal_();
    pttl = 2000;

    FalsePexpire

//kv type
    client->set(key,val);
    PTTL(-1)
    OKPexpire

    sleep(( pttl+1000 )/1000);
    FalsePexpire

    pttl = 10000;
    client->setx(key,val, 5);
    OKPexpire

    client->getset(key, val, &getVal);
    PTTL(-1)
    OKPexpire

    client->del(key);
    PTTL(-2)
    FalsePexpire

    pttl = LONG_LONG_MAX;
    client->set(key, "val");
    client->pexpire(key, pttl, &ret);
    getVal.clear();
    client->get(key, &getVal);
    EXPECT_EQ("val", getVal)<<"pexpire 64bit INT_MAX(9223372036854775807) ttl should not del key"<<endl;
    sleep(1);
    getVal.clear();
    client->get(key, &getVal);
    EXPECT_EQ("val", getVal)<<"pexpire 64bit INT_MAX(9223372036854775807) ttl should not del key after 1 sec"<<endl;;
    client->del(key);
}

TEST_F(KeysTest, Test_keys_pttl) {
    key = GetRandomKey_();
    field = GetRandomField_();
    val = GetRandomVal_();
    score = GetRandomDouble_();
    pttl = 1000;

//when pttl exceed, key was deleted.
        pttl += 1000;
        client->del(key);
        PTTL(-2)
        client->set(key,val);
        PTTL(-1)
        client->pexpire(key, pttl, &ret);
        NEAR_PTTL(pttl)
        sleep(( pttl-1000 )/1000);
        s = client->get(key, &getVal);
        ASSERT_FALSE(s.not_found())<<"this key should be found!"<<endl;
        sleep(2);
        s = client->get(key, &getVal);
        ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<endl;
        PTTL(-2)

//Pexpire pttl <= 0, then key was deleted
        pttl += 1000;
        client->del(key);
        PTTL(-2)
        client->set(key,val);
        client->pexpire(key, -1000, &ret);
        s = client->get(key, &getVal);

        ASSERT_TRUE(s.not_found())<<"this key should be not found!"<<s.code()<<endl;
        PTTL(-2)
}
