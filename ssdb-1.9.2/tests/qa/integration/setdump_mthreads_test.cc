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

class SetDumpMthreadsTest : public SSDBTest
{
    public:
        ssdb::Status s;
        static const int BIG_KEY_NUM = 10000;
        static const int setThreads = 3;
        static const int threadsNum = 15;
        std::vector<std::string> list, keys, keys1, keys2;
        std::map<std::string, std::string> kvs;
        std::map<std::string, double> items;
        string key, restorekey, val, getVal, field;
        bool midStateFlag;
        uint32_t keysNum;
        Type type;
        int64_t ret;
        double score, getScore;
        pthread_t bg_tid[threadsNum];
        void * status;

        static void* write_thread_func(void *arg);
        static void* read_thread_func(void *arg);
};

void* SetDumpMthreadsTest::write_thread_func(void *arg) {
    if(LDEBUG) cout<<"write_thread_func start!"<<endl; 
    SetDumpMthreadsTest* mthreadsTest = (SetDumpMthreadsTest*)arg;
    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL) {
        cout<<"fail to connect to server in mset_thread_func!";
        return (void *)NULL;
    }

    switch (mthreadsTest->type){
        case Type::HASH:
            mthreadsTest->s = tmpclient->multi_hset(mthreadsTest->key, mthreadsTest->kvs);
            break;
        case Type::LIST:
            mthreadsTest->s = tmpclient->qpush_back(mthreadsTest->key, mthreadsTest->keys);
            break;
        case Type::SET:
            mthreadsTest->s = tmpclient->sadd(mthreadsTest->key, mthreadsTest->keys);
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

void* SetDumpMthreadsTest::read_thread_func(void *arg) {
    ssdb::Status s_dump, s_restore;
    SetDumpMthreadsTest* mthreadsTest = (SetDumpMthreadsTest*)arg;
    if(mthreadsTest->midStateFlag == true)
        return (void *)NULL;

    ssdb::Client *tmpclient = ssdb::Client::connect(mthreadsTest->ip.data(), mthreadsTest->port);
    if(tmpclient == NULL) {
        cout<<"fail to connect to server in read_thread_func!";
        return (void *)NULL;
    }
    int64_t ret;

    string dumpVal, restoreVal, getVal;
    std::vector<std::string> tmplist;
    tmplist.clear();
    s_dump = tmpclient->dump(mthreadsTest->key, &dumpVal);
    if(s_dump.ok()) {
        s_restore = tmpclient->restore(mthreadsTest->restorekey, 0, dumpVal, "replace", &restoreVal);
        if(s_restore.ok()){
            switch (mthreadsTest->type){
                case Type::HASH:
                    tmpclient->hsize(mthreadsTest->restorekey, &ret);
                    mthreadsTest->s = tmpclient->multi_hget(mthreadsTest->restorekey, mthreadsTest->keys, &tmplist);

                    for(int n = 0;n < tmplist.size()/2; n++){
                        if(mthreadsTest->field+itoa(n)!=tmplist[0+2*n]||
                                mthreadsTest->val+itoa(n)!=tmplist[1+2*n]) {
                            cout<<n<<":"<<tmplist[0+2*n]<<":"<<tmplist[1+2*n]<<endl;
                            mthreadsTest->midStateFlag = true;
                            break;
                        }
                    }
                    break;
                case Type::LIST:
                    tmpclient->qsize(mthreadsTest->restorekey, &ret);
                    mthreadsTest->s = tmpclient->qslice(mthreadsTest->restorekey, 0, -1, &tmplist);
                    break;
                case Type::SET:
                    tmpclient->scard(mthreadsTest->restorekey, &ret);
                    mthreadsTest->s = tmpclient->smembers(mthreadsTest->restorekey, &tmplist);
                    break;
                case Type::ZSET:
                    tmpclient->zsize(mthreadsTest->restorekey, &ret);
                    mthreadsTest->s = tmpclient->zrange(mthreadsTest->restorekey, 0, -1, &tmplist);
                    break;
            }

            if(tmplist.size()/mthreadsTest->keysNum>setThreads || 0 != ret%mthreadsTest->keysNum || 0 != tmplist.size()%mthreadsTest->keysNum){
                cout<<"restore key size error!"<<ret<<":"<<tmplist.size()<<endl;
                mthreadsTest->midStateFlag = true;
            }

            if(!mthreadsTest->s.ok()) {
                cout<<"read fail!"<<mthreadsTest->s.code()<<endl;
                mthreadsTest->midStateFlag = true;
    }
        } else {
            cout<<"restore key fail!"<<endl;
            mthreadsTest->midStateFlag = true;
        }
    } else if(!s_dump.not_found()){
        cout<<"dump key error!"<<endl;
        mthreadsTest->midStateFlag = true;
    }


    delete tmpclient;
    tmpclient = NULL;
    return (void *)NULL;
}

TEST_F(SetDumpMthreadsTest, Test_hmset_dump_restore_mthreads) {
    type = Type::HASH;
    key = "hkey";
    restorekey = "hrestorekey";
    field = "field";
    val = "val";
    keysNum = BIG_KEY_NUM;
    keys.clear();
    kvs.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++) {
        keys.push_back(field+itoa(n));
        kvs.insert(std::make_pair(field+itoa(n), val+itoa(n)));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++) {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = setThreads*2; n < threadsNum; n++) {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(1000);
    }

    for(int n = 0; n < threadsNum; n++)
    {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;
    s = client->del(key);
    s = client->del(restorekey);
}

TEST_F(SetDumpMthreadsTest, Test_rpush_dump_restore_mthreads) {
    type = Type::LIST;
    key = "lrkey";
    restorekey = "lrestorekey";
    val = "val";
    keysNum = BIG_KEY_NUM;
    keys.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++) {
        keys.push_back(val+itoa(n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++) {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(200);
    }

    for(int n = setThreads*2; n < threadsNum; n++) {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(500);
    }

    for(int n = 0; n < threadsNum; n++) {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;
    //TODO compare

    s = client->del(key);
    s = client->del(restorekey);
}

TEST_F(SetDumpMthreadsTest, Test_sadd_dump_restore_mthreads) {
    type = Type::SET;
    key = "skey";
    restorekey = "srestorekey";
    val = "val";
    keysNum = BIG_KEY_NUM*2;
    keys.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++) {
        keys.push_back(val+itoa(n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++) {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++) {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++) {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    s = client->del(key);
    s = client->del(restorekey);
}

TEST_F(SetDumpMthreadsTest, Test_zadd_dump_restore_mthreads) {
    type = Type::ZSET;
    key = "zkey";
    restorekey = "zrestorekey";
    field = "field";
    score = 0;
    keysNum = BIG_KEY_NUM*3;
    keys.clear();
    items.clear();
    midStateFlag = false;
    for(int n = 0;n < keysNum; n++) {
        keys.push_back(field+itoa(n));
        items.insert(std::make_pair(field+itoa(n), score+n));
    }

    s = client->del(key);

    for(int n = 0; n < setThreads; n++) {
        pthread_create(&bg_tid[n], NULL, &write_thread_func, this);
        pthread_create(&bg_tid[n+setThreads], NULL, &read_thread_func, this);
        usleep(1*1000);
    }

    for(int n = setThreads*2; n < threadsNum; n++) {
        pthread_create(&bg_tid[n], NULL, &read_thread_func, this);
        usleep(2*1000);
    }

    for(int n = 0; n < threadsNum; n++) {
        pthread_join(bg_tid[n], &status);
    }
    EXPECT_EQ(false, midStateFlag)<<"Multi threads Read mid state when write key!" <<endl;

    s = client->del(key);
    s = client->del(restorekey);
}
