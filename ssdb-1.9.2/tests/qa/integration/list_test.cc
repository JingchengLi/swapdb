#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <sstream>
#include "SSDB_client.h"
#include "gtest/gtest.h"
#include "ssdb_test.h"
using namespace std;

class ListTest : public SSDBTest
{
    public:
        ssdb::Status s;
        std::vector<std::string> list, getList, comList;
        string key, val, getVal;
        uint16_t keysNum;
        int64_t ret;
};

TEST_F(ListTest, Test_list_qpush_front) {
#define OKQpush_front(n) s = client->qpush_front(key, list, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to qpush_front key!"<<endl;\
    s = client->qsize(key, &ret);\
    ASSERT_EQ(n, ret)<<"qsize get not equal:"<<key<<endl;

#define FalseQpush_front s = client->qpush_front(key, list, &ret);\
    ASSERT_TRUE(s.error())<<"this key should qpush_front fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_();
        client->del(key);
        client->qpush_front(key, val+"0", &ret);
        client->qpush_front(key, val, &ret);
        getList.clear();
        s = client->qpop_front(key, 1, &getList);
        ASSERT_EQ(val, getList[0].data())<<"val should be pop up:"<<key<<endl;
        s = client->qclear(key);
    }

    // Some random keys
    keysNum = 30;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    list.clear(); 
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
        OKQpush_front(n+1)
        list.pop_back(); 
    }
    getList.clear();
    s = client->qslice(key, 0, -1, &getList);
    std::vector<string>::iterator getit = getList.begin();
    for(int n = 0; getit != getList.end(); getit++, n++)
        ASSERT_EQ(val+itoa(keysNum-n-1), *getit)<<"list not equal:"<<endl;

    key = GetRandomKey_(); 
    val = GetRandomVal_();
    list.clear();
    comList.clear();
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
        comList.push_back(val+itoa(n));
    }
    OKQpush_front(keysNum);
    list.clear();
    for(int n = keysNum; n < keysNum*2; n++)
    {
        list.push_back(val+itoa(n)); 
        comList.push_back(val+itoa(n));
    }
    OKQpush_front(keysNum*2);
    std::vector<string>::reverse_iterator qit = comList.rbegin();
    getList.clear();
    s = client->qslice(key, 0, -1, &getList);
    getit = getList.begin();
    for(; qit != comList.rend(), getit != getList.end(); qit++,getit++)
    {
        ASSERT_EQ(*qit, *getit)<<"list not equal:"<<key<<endl;
    }

    //other types key
    s = client->del(key);
    s = client->set(key, val);
    FalseQpush_front
    s = client->del(key);
}

TEST_F(ListTest, Test_list_qpush_back) {
#define OKQpush_back(n) s = client->qpush_back(key, list, &ret);\
    ASSERT_TRUE(s.ok())<<"fail to qpush_back key!"<<endl;\
    s = client->qsize(key, &ret);\
    ASSERT_EQ(n, ret)<<"qsize get not equal:"<<key<<endl;

#define FalseQpush_back s = client->qpush_back(key, list, &ret);\
    ASSERT_TRUE(s.error())<<"this key should qpush_back fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        s = client->del(key);
        client->qpush_back(key, val+"0", &ret);
        client->qpush_back(key, val, &ret);
        getList.clear();
        s = client->qpop_back(key, 1, &getList);
        ASSERT_EQ(val, getList[0])<<"val should be pop up"<<endl;
        s = client->qclear(key);
    } 

    // Some random keys
    keysNum = 30;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    list.clear(); 
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
        OKQpush_back(n+1)
        list.pop_back(); 
    }
    getList.clear();
    s = client->qslice(key, 0, -1, &getList);
    std::vector<string>::iterator getit = getList.begin();
    for(int n = 0; getit != getList.end(); getit++, n++)
        ASSERT_EQ(val+itoa(n), *getit)<<"list not equal:"<<endl;

    key = GetRandomKey_(); 
    val = GetRandomVal_();
    list.clear();
    comList.clear();
    for(int n = 0; n < keysNum; n++)
    {
        list.push_back(val+itoa(n)); 
        comList.push_back(val+itoa(n));
    }
    OKQpush_back(keysNum);
    list.clear();
    for(int n = keysNum; n < keysNum*2; n++)
    {
        list.push_back(val+itoa(n)); 
        comList.push_back(val+itoa(n));
    }
    OKQpush_back(keysNum*2);
    std::vector<string>::iterator qit = comList.begin();
    getList.clear();
    s = client->qslice(key, 0, -1, &getList);
    getit = getList.begin();
    for(; qit != comList.end(), getit != getList.end(); qit++,getit++)
    {
        ASSERT_EQ(*qit, *getit)<<"list not equal:"<<key<<endl;
    }

    //other types key
    s = client->del(key);
    s = client->set(key, val);
    FalseQpush_back
    s = client->del(key);
}

TEST_F(ListTest, Test_list_qpop_front) {
#define OKQpop_front(n) getList.clear();\
    s = client->qpop_front(key, n, &getList);\
    ASSERT_TRUE(s.ok())<<"fail to qpop_front key!"<<endl;\
    ASSERT_EQ(n, getList.size())<<"qpop size get not equal:"<<key<<endl;

#define FalseQpop_front(n) s = client->qpop_front(key, n, &getList);\
    ASSERT_TRUE(s.not_found())<<"this key should qpop_front fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        list.clear();
        list.push_back(val);
        s = client->del(key);
        s = client->qpush_front(key, list, &ret);
        OKQpop_front(1)
        FalseQpop_front(1)
    } 

    // Some random keys
    keysNum = 30;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    for(int n = 0; n < keysNum; n++)
    {
        s = client->qpush_back(key, val+itoa(n), &ret);
    }
    OKQpop_front(keysNum/2)
    for(int n = 0; n < keysNum/2; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }

    OKQpop_front(( keysNum+1 )/2)
    for(int n = keysNum/2; n < keysNum; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }
}

TEST_F(ListTest, Test_list_qpop_back) {
#define OKQpop_back(n) getList.clear();\
    s = client->qpop_back(key, n, &getList);\
    ASSERT_TRUE(s.ok())<<"fail to qpop_back key!"<<endl;\
    ASSERT_EQ(n, getList.size())<<"qpop size get not equal:"<<key<<endl;

#define FalseQpop_back(n) s = client->qpop_back(key, n, &getList);\
    ASSERT_TRUE(s.not_found())<<"this key should qpop_back fail!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        list.clear();
        list.push_back(val);
        s = client->del(key);
        s = client->qpush_back(key, list, &ret);
        OKQpop_back(1)
        FalseQpop_back(1)
    } 

    // Some random keys
    keysNum = 30;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    for(int n = 0; n < keysNum; n++)
    {
        s = client->qpush_front(key, val+itoa(n), &ret);
    }
    OKQpop_back(keysNum/2)
    for(int n = 0; n < keysNum/2; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }

    OKQpop_back(( keysNum+1 )/2)
    for(int n = keysNum/2; n < keysNum; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }
}

TEST_F(ListTest, Test_list_qsize) {
#define OKQsize(num) s = client->qsize(key, &ret);\
    ASSERT_EQ(ret, num)<<"fail to qsize key!"<<key<<endl;\
    ASSERT_TRUE(s.ok())<<"qsize key not ok!"<<endl;

    // Some special keys
    for(vector<string>::iterator it = Keys.begin(); it != Keys.end(); it++)
    {
        key = *it;
        val = GetRandomVal_(); 
        list.clear();
        list.push_back(val);
        s = client->del(key);
        OKQsize(0)
        s = client->qpush_back(key, list, &ret);
        OKQsize(1)
        s = client->qclear(key);
    }

    key = GetRandomKey_(); 
    val = GetRandomVal_(); 

    list.clear();
    s = client->del(key);
    for(int n = 0; n < 10; n++)
    {
        list.push_back(val+itoa(n));
        s = client->qpush_back(key, list, &ret);
        list.pop_back();
        OKQsize(n+1)
    }

    for(int n = 0; n < 10; n++)
    {
        s = client->qpop_back(key, 1, &getList);
        OKQsize(10-n-1)
    }

    s = client->qclear(key);
}

TEST_F(ListTest, Test_list_qslice) {

    // Some random keys
    keysNum = 30;
    key = GetRandomKey_(); 
    val = GetRandomVal_();
    s = client->del(key);
    for(int n = 0; n < keysNum; n++)
    {
        s = client->qpush_back(key, val+itoa(n), &ret);
    }
    s = client->qslice(key, 0, keysNum/2, &getList);
    for(int n = 0; n <= keysNum/2; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }
    getList.clear();
    s = client->qslice(key, keysNum/2+1, keysNum-1, &getList);

    for(int n = keysNum/2+1; n < keysNum; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n-keysNum/2-1]);
    }

    getList.clear();
    s = client->qslice(key, keysNum-1, keysNum/2+1, &getList);
    ASSERT_EQ(0, getList.size());

    getList.clear();
    s = client->qslice(key, keysNum, keysNum-1, &getList);
    ASSERT_EQ(0, getList.size());

    getList.clear();
    s = client->qslice(key, keysNum+1, keysNum+10, &getList);
    ASSERT_EQ(0, getList.size());

    /*negative index*/
    getList.clear();
    s = client->qslice(key, -keysNum, -keysNum/2, &getList);
    for(int n = 0; n <= keysNum/2; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }

    getList.clear();
    s = client->qslice(key, -keysNum/2+1, -1, &getList);
    for(int n = keysNum/2+1; n < keysNum; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n-keysNum/2-1]);
    }

    getList.clear();
    s = client->qslice(key, -keysNum/2-1, -keysNum+1, &getList);
    ASSERT_EQ(0, getList.size());

    getList.clear();
    s = client->qslice(key, -keysNum-1, -keysNum, &getList);
    ASSERT_EQ(0, getList.size());

    getList.clear();
    s = client->qslice(key, -keysNum-10, -keysNum-1, &getList);
    ASSERT_EQ(0, getList.size()); 

    /*positive adn negative index*/
    getList.clear();
    s = client->qslice(key, -keysNum-1, keysNum/2, &getList);
    for(int n = 0; n <= keysNum/2; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n]);
    }

    getList.clear();
    s = client->qslice(key, keysNum/2+1, -1, &getList);
    for(int n = keysNum/2+1; n < keysNum; n++)
    {
        ASSERT_EQ(val+itoa(n), getList[n-keysNum/2-1]);
    }

    getList.clear();
    s = client->qslice(key, -keysNum/2-1, 1, &getList);
    ASSERT_EQ(0, getList.size());

    client->qclear(key);
}
