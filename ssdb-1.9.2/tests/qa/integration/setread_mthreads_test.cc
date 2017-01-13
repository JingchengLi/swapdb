#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
#include <time.h>
#include <algorithm>

//log info when set true
const bool LDEBUG = false;

using namespace std;

enum Type{ STRING, HASH, LIST, SET, ZSET };

class SetReadMthreadsTest : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int BIG_KEY_NUM = 10000;
        static const int setThreads = 3;
        static const int threadsNum = 25;
        std::vector<std::string> list, keys, keys1, keys2;
        std::map<std::string, std::string> kvs;
        std::map<std::string, double> items;
        string key, val, getVal, field;
        bool midStateFlag;
        uint32_t keysNum;
        Type type;
        int64_t ret;
        double score, getScore;
        pthread_t bg_tid[threadsNum];
        void * status;

        static void* write_thread_func(void *arg);
        static void* read_thread_func(void *arg);
        static void* read_thread_func_2(void *arg);
        static void* read_thread_func_3(void *arg);
        static void* read_thread_func_4(void *arg);
};

void* SetReadMthreadsTest::write_thread_func(void *arg) {
    if(LDEBUG) cout<<"write_thread_func start!"<<endl; 
    SetReadMthreadsTest* mthreadsTest = (SetReadMthreadsTest*)arg;
    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in mset_thread_func!";
        return (void *)NULL;
    }

    switch (mthreadsTest->type){
        case Type::STRING:
            mthreadsTest->s = tmpclient->multi_set(mthreadsTest->kvs);
            break;
        case Type::HASH:
            mthreadsTest->s = tmpclient->multi_hset(mthreadsTest->key, mthreadsTest->kvs);
            break;
        case Type::LIST:
            if(0 == mthreadsTest->key.compare("lrkey"))
                mthreadsTest->s = tmpclient->qpush_back(mthreadsTest->key, mthreadsTest->keys);
            else
                mthreadsTest->s = tmpclient->qpush_front(mthreadsTest->key, mthreadsTest->keys);
            break;
        case Type::SET:
            if(0 == mthreadsTest->key.compare("skey"))
                mthreadsTest->s = tmpclient->sadd(mthreadsTest->key, mthreadsTest->keys);
            else{
                mthreadsTest->s = tmpclient->sadd("s1", mthreadsTest->keys1);
                mthreadsTest->s = tmpclient->sadd("s2", mthreadsTest->keys2);
            }
            break;
        case Type::ZSET:
            mthreadsTest->s = tmpclient->zset(mthreadsTest->key, mthreadsTest->items);
            break;
    }

    if(!mthreadsTest->s.ok())
        cout<<"write fail!"<<mthreadsTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    if(LDEBUG) cout<<"write_thread_func complete!"<<endl; 
    return (void *)NULL;
}

void* SetReadMthreadsTest::read_thread_func(void *arg) {
    ssdb::Status s_before, s_after;
    SetReadMthreadsTest* mthreadsTest = (SetReadMthreadsTest*)arg;
    if(mthreadsTest->midStateFlag == true)
        return (void *)NULL;

    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in read_thread_func!";
        return (void *)NULL;
    }

    std::vector<std::string> tmplist;
    int64_t size_before, size_after, isflag_before, isflag_after, dflag = 1;//some return list size is twice than elements number.
    int randno = rand()%mthreadsTest->keysNum;
    string randkey = mthreadsTest->key+itoa(randno);
    string randfield = mthreadsTest->field+itoa(randno);
    string randval = mthreadsTest->val+itoa(randno);
    tmplist.clear();
    switch (mthreadsTest->type){
        case Type::STRING:
            dflag = 2;
            s_before = tmpclient->get(randkey, &mthreadsTest->getVal);
            size_before = 0;
            mthreadsTest->s = tmpclient->multi_get(mthreadsTest->keys, &tmplist);
            s_after = tmpclient->get(randkey, &mthreadsTest->getVal);
            size_before = mthreadsTest->keysNum;
            break;
        case Type::HASH:
            dflag = 2;
            s_before = tmpclient->hget(mthreadsTest->key, randfield, &mthreadsTest->getVal);
            tmpclient->hsize(mthreadsTest->key, &size_before);
            mthreadsTest->s = tmpclient->multi_hget(mthreadsTest->key, mthreadsTest->keys, &tmplist);
            s_after = tmpclient->hget(mthreadsTest->key, randfield, &mthreadsTest->getVal);
            tmpclient->hsize(mthreadsTest->key, &size_after);
            break;
        case Type::LIST:
            dflag = 1;
            s_before = tmpclient->qget(mthreadsTest->key, randno, &mthreadsTest->getVal);
            tmpclient->qsize(mthreadsTest->key, &size_before);
            mthreadsTest->s = tmpclient->qslice(mthreadsTest->key, 0, -1, &tmplist);
            s_after = tmpclient->qget(mthreadsTest->key, randno, &mthreadsTest->getVal);
            tmpclient->qsize(mthreadsTest->key, &size_after);
            break;
        case Type::SET:
            dflag = 1;
            tmpclient->sismember(mthreadsTest->key, randval, &isflag_before);
            tmpclient->scard(mthreadsTest->key, &size_before);
            mthreadsTest->s = tmpclient->smembers(mthreadsTest->key, &tmplist);
            tmpclient->sismember(mthreadsTest->key, randval, &isflag_after);
            tmpclient->scard(mthreadsTest->key, &size_after);
            break;
        case Type::ZSET:
            dflag = 2;
            s_before = tmpclient->zget(mthreadsTest->key, randfield, &mthreadsTest->getScore);
            tmpclient->zsize(mthreadsTest->key, &size_before);
            mthreadsTest->s = tmpclient->zrange(mthreadsTest->key, 0, -1, &tmplist);
            s_after = tmpclient->zget(mthreadsTest->key, randfield, &mthreadsTest->getScore);
            tmpclient->zsize(mthreadsTest->key, &size_after);
            break;
    }

    switch (mthreadsTest->type){
        case Type::STRING:
        case Type::HASH:
        case Type::ZSET:
            if(0 == tmplist.size()) {
                if(!s_before.not_found()||0 != size_before) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be not found!"<<s_before.code()<<size_before<<endl;
                }
            } else if(mthreadsTest->keysNum == tmplist.size()/dflag){
                if(!s_after.ok()||mthreadsTest->keysNum != size_after) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be ok!"<<s_after.code()<<size_after<<endl;
                }
            }
            break;
        case Type::LIST:
            if(0 == tmplist.size()) {
                if(!s_before.not_found()||0 != size_before) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be not found!"<<s_before.code()<<size_before<<endl;
                }
            } else if(0 == tmplist.size()/dflag%mthreadsTest->keysNum){
                if(!s_after.ok()|| 0 != size_after%mthreadsTest->keysNum) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be ok!"<<s_after.code()<<size_after<<endl;
                }
            }
            break;
        case Type::SET:
            if(0 == tmplist.size()) {
                if(isflag_before||0 != size_before) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be not found!"<<s_before.code()<<size_before<<endl;
                }
            } else if(mthreadsTest->keysNum == tmplist.size()/dflag){
                if(!isflag_after||mthreadsTest->keysNum != size_after) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key "<<randno<<" should be ok!"<<s_after.code()<<size_after<<endl;
                }
            }
            break;
    }

    if(LDEBUG) cout<<"Get list size is:"<<tmplist.size()/dflag<<endl;

    if(Type::LIST != mthreadsTest->type){
        if(mthreadsTest->keysNum != tmplist.size()/dflag && 0 != tmplist.size()){
            cout<<"Read mid state list size is:"<<tmplist.size()/dflag<<endl;
            mthreadsTest->midStateFlag = true;
        }
    } else {
        if(0 != tmplist.size()/dflag%mthreadsTest->keysNum){
            cout<<"Read mid state list size is:"<<tmplist.size()/dflag<<endl;
            mthreadsTest->midStateFlag = true;
        }
    }

    if(!mthreadsTest->s.ok())
        cout<<"read fail!"<<mthreadsTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    return (void *)NULL;
}

void* SetReadMthreadsTest::read_thread_func_2(void *arg) {
    ssdb::Status s_before, s_after;
    SetReadMthreadsTest* mthreadsTest = (SetReadMthreadsTest*)arg;
    if(mthreadsTest->midStateFlag == true)
        return (void *)NULL;
    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in read_thread_func!";
        return (void *)NULL;
    }

    std::vector<std::string> tmplist;
    int64_t isflag_before, isflag_after, dflag = 1;//some return list size is twice than elements number.
    int randno = rand()%mthreadsTest->keysNum;
    string randkey = mthreadsTest->key+itoa(randno);
    string randfield = mthreadsTest->field+itoa(randno);
    string randval = mthreadsTest->val+itoa(randno);
    tmplist.clear();
    switch (mthreadsTest->type){
        case Type::HASH:
            dflag = 2;
            s_before = tmpclient->hexists(mthreadsTest->key, randfield, &isflag_before);
            mthreadsTest->s = tmpclient->hgetall(mthreadsTest->key, &tmplist);
            s_after = tmpclient->hexists(mthreadsTest->key, randfield, &isflag_after);
            break;
        case Type::ZSET:
            dflag = 2;
            s_before = tmpclient->zrank(mthreadsTest->key, randfield, &mthreadsTest->ret);
            mthreadsTest->s = tmpclient->zrrange(mthreadsTest->key, 0, -1, &tmplist);
            s_after = tmpclient->zrank(mthreadsTest->key, randfield, &mthreadsTest->ret);
            break;
        case Type::SET:
            mthreadsTest->s = tmpclient->sunion(mthreadsTest->keys, &tmplist);
            break;
    }

    switch (mthreadsTest->type){
        case Type::HASH:
            if(0 == tmplist.size()) {
                if(isflag_before) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key should be not found!"<<randno<<s_before.code()<<endl;
                }
            } else if(mthreadsTest->keysNum == tmplist.size()/dflag){
                if(!isflag_after) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key should be ok!"<<randno<<s_after.code()<<endl;
                }
            }
            break;
        case Type::ZSET:
            if(0 == tmplist.size()) {
                if(!s_before.not_found()) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key should be not found!"<<randno<<s_before.code()<<endl;
                }
            } else if(mthreadsTest->keysNum == tmplist.size()/dflag){
                if(!s_after.ok()) {
                    mthreadsTest->midStateFlag = true;
                    cout<<"this key should be ok!"<<randno<<s_after.code()<<endl;
                }
            }
            break;
    }

    if(LDEBUG) cout<<"Get list size is:"<<tmplist.size()/dflag<<endl;

    if(0 != tmplist.size()/dflag%mthreadsTest->keysNum){
        cout<<"Read mid state list size is:"<<tmplist.size()/dflag<<endl;
        mthreadsTest->midStateFlag = true;
    }

    if(!mthreadsTest->s.ok()) cout<<"read fail!"<<mthreadsTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    return (void *)NULL;
}

void* SetReadMthreadsTest::read_thread_func_3(void *arg) {
    ssdb::Status s_before, s_after;
    SetReadMthreadsTest* mthreadsTest = (SetReadMthreadsTest*)arg;
    if(mthreadsTest->midStateFlag == true)
        return (void *)NULL;
    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in read_thread_func!";
        return (void *)NULL;
    }

    std::vector<std::string> tmplist;
    int64_t ret, isflag, dflag = 1;//some return list size is twice than elements number.
    int randno = rand()%mthreadsTest->keysNum;
    string randkey = mthreadsTest->key+itoa(randno);
    string randfield = mthreadsTest->field+itoa(randno);
    string randval = mthreadsTest->val+itoa(randno);
    tmplist.clear();
    switch (mthreadsTest->type){
        case Type::HASH:
            mthreadsTest->s = tmpclient->hkeys(mthreadsTest->key, "", "", 20000000, &tmplist);
            ret = tmplist.size();
            break;
        case Type::SET:
            mthreadsTest->s = tmpclient->sunionstore(mthreadsTest->key, mthreadsTest->keys, &ret);
            break;
    }

    if(LDEBUG) cout<<"Get list size is:"<<ret/dflag<<endl;

    if(0 != ret/dflag%mthreadsTest->keysNum){
        cout<<"Read mid state list size is:"<<ret/dflag<<endl;
        mthreadsTest->midStateFlag = true;
    }

    if(!mthreadsTest->s.ok()) cout<<"read fail!"<<mthreadsTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    return (void *)NULL;
}

void* SetReadMthreadsTest::read_thread_func_4(void *arg) {
    ssdb::Status s_before, s_after;
    SetReadMthreadsTest* mthreadsTest = (SetReadMthreadsTest*)arg;
    if(mthreadsTest->midStateFlag == true)
        return (void *)NULL;
    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL)
    {
        cout<<"fail to connect to server in read_thread_func!";
        return (void *)NULL;
    }

    std::vector<std::string> tmplist;
    int64_t isflag, dflag = 1;//some return list size is twice than elements number.
    int randno = rand()%mthreadsTest->keysNum;
    string randkey = mthreadsTest->key+itoa(randno);
    string randfield = mthreadsTest->field+itoa(randno);
    string randval = mthreadsTest->val+itoa(randno);
    tmplist.clear();
    switch (mthreadsTest->type){
        case Type::HASH:
            dflag = 1;
            mthreadsTest->s = tmpclient->hvals(mthreadsTest->key, "", "", 20000000, &tmplist);
            break;
    }

    if(LDEBUG) cout<<"Get list size is:"<<tmplist.size()/dflag<<endl;

    if(mthreadsTest->keysNum != tmplist.size()/dflag && 0 != tmplist.size()){
        cout<<"Read mid state list size is:"<<tmplist.size()/dflag<<endl;
        mthreadsTest->midStateFlag = true;
    }

    if(!mthreadsTest->s.ok()) cout<<"read fail!"<<mthreadsTest->s.code()<<endl;

    delete tmpclient;
    tmpclient = NULL;
    return (void *)NULL;
}

TEST_F(SetReadMthreadsTest, Test_mset_get_mget_mthreads) {
    type = Type::STRING;
    key = "{key}";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
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
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(10*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->multi_get(keys, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->get(key+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<key+itoa(n)<<endl;
    }
    s = client->multi_del(keys);
    ASSERT_EQ(keysNum, list.size()/2)<<"Write fail at last:"<<list.size()/2<<endl;
}

TEST_F(SetReadMthreadsTest, Test_hmset_hget_hlen_hmget_mthreads) {
    type = Type::HASH;
    key = "hkey";
    field = "field";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        kvs.insert(std::make_pair(field+itoa(n), val+itoa(n)));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->multi_hget(key, keys, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->hget(key, field+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size()/2)<<"Write fail at last:"<<list.size()/2<<endl;
}

TEST_F(SetReadMthreadsTest, Test_hmset_hexists_hgetall_mthreads) {
    type = Type::HASH;
    key = "hkey";
    field = "field";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        kvs.insert(std::make_pair(field+itoa(n), val+itoa(n)));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_2, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }

    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->hgetall(key, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->hget(key, field+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size()/2)<<"Write fail at last:"<<list.size()/2<<endl;
}

TEST_F(SetReadMthreadsTest, Test_hmset_hkeys_mthreads) {
    type = Type::HASH;
    key = "hkey";
    field = "field";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        kvs.insert(std::make_pair(field+itoa(n), val+itoa(n)));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_3, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }

    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->hkeys(key, "", "", 20000000, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->hget(key, field+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_hmset_hvals_mthreads) {
    type = Type::HASH;
    key = "hkey";
    field = "field";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        kvs.insert(std::make_pair(field+itoa(n), val+itoa(n)));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_4, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }

    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->hvals(key, "", "", 20000000, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->hget(key, field+itoa(n), &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_rpush_lindex_llen_lrange_mthreads) {
    type = Type::LIST;
    key = "lrkey";
    val = "val";
    keysNum = BIG_KEY_NUM;
    keys.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(val+itoa(n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(200);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->qslice(key, 0, -1, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->qget(key, n, &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum*setThreads, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_lpush_lindex_llen_lrange_mthreads) {
    type = Type::LIST;
    key = "llkey";
    val = "val";
    keysNum = BIG_KEY_NUM;
    keys.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(val+itoa(n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(200);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->qslice(key, 0, -1, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->qget(key, keysNum-n-1, &getVal);
        if(getVal != val+itoa(n))
            cout<<"not_equal:"<<val+itoa(n)<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum*setThreads, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_sadd_sismember_scard_smembers_mthreads) {
    type = Type::SET;
    key = "skey";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(val+itoa(n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->smembers(key, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->sismember(key, val+itoa(n), &ret);
        if(true != ret)
            cout<<val+itoa(n)<<" not exist in set!"<<endl;
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_sadd_sunion_mthreads) {
    type = Type::SET;
    key = "sunionkey";
    val = "val";
    keysNum = BIG_KEY_NUM/2;
    keys.clear();
    keys.push_back("s1");
    keys.push_back("s2");
    keys1.clear();
    keys2.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys1.push_back(val+itoa(n));
        keys2.push_back(val+itoa(n+keysNum));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum/2; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_2, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum/2; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->sunion(keys, &list);
    std::sort(list.begin(), list.end());
    for(int n = 0; n < keysNum; n++) {
        if(!std::binary_search(list.begin(), list.end(), val+itoa(n))) {
            cout<<val+itoa(n)<<" not exist in set!"<<endl;
            break;
        }
    }
    s = client->del("s1");
    s = client->del("s2");
    ASSERT_EQ(keysNum*2, list.size())<<"Write fail at last:"<<list.size()<<endl;
}

TEST_F(SetReadMthreadsTest, Test_sadd_sunionstore_mthreads) {
    type = Type::SET;
    key = "sunionstorekey";
    val = "val";
    keysNum = BIG_KEY_NUM/2;
    keys.clear();
    keys.push_back("s1");
    keys.push_back("s2");
    keys1.clear();
    keys2.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys1.push_back(val+itoa(n));
        keys2.push_back(val+itoa(n+keysNum));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum/2; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_2, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum/2; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->sunionstore(key, keys, &ret);
    ASSERT_EQ(keysNum*2, ret)<<"Write fail at last"<<endl;
    for(int n = 0; n < keysNum*2; n++) {
        client->sismember(key, val+itoa(n), &ret);
        if(true != ret) {
            cout<<val+itoa(n)<<" not exist in set!"<<endl;
            break;
        }
    }
    s = client->del("s1");
    s = client->del("s2");
    s = client->del(key);
}

TEST_F(SetReadMthreadsTest, Test_zadd_zscore_zcard_zrange_mthreads) {
    type = Type::ZSET;
    key = "zkey";
    field = "field";
    score = 0;
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    items.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        items.insert(std::make_pair(field+itoa(n), score+n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->zrange(key, 0, -1, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->zget(key, field+itoa(n), &getScore);
        ASSERT_NEAR(n, getScore, eps);
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size()/2)<<"Write fail at last:"<<list.size()/2<<endl;
}

TEST_F(SetReadMthreadsTest, Test_zadd_zrank_zrrange_mthreads) {
    type = Type::ZSET;
    key = "zkey";
    field = "field";
    score = 0;
    keysNum = BIG_KEY_NUM/2;
    keys.clear();
    items.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++)
    {
        keys.push_back(field+itoa(n));
        items.insert(std::make_pair(field+itoa(n), score+n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++)
    {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = setThreads*2; n < threadsNum/2; n++)
    {
        pthread_create(&bg_tid[n], NULL, &read_thread_func_2, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum/2; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    list.clear();
    s = client->zrrange(key, 0, -1, &list);
    for(int n = 0; n < keysNum; n++)
    {
        client->zget(key, field+itoa(n), &getScore);
        ASSERT_NEAR(n, getScore, eps);
    }
    s = client->del(key);
    ASSERT_EQ(keysNum, list.size()/2)<<"Write fail at last:"<<list.size()/2<<endl;
}
