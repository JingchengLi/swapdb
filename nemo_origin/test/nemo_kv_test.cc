#include <string>
#include <vector>
#include <sys/time.h>
#include <cstdlib>
#include <cstdlib>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

//#include "stdint.h"
#include "nemo_kv_test.h"
using namespace std;


TEST_F(NemoKVTest, TestSet)
{
	log_message("============================KVTEST START===========================");
	log_message("============================KVTEST START===========================");
	log_message("========TestSet========");
	string keyMin, keyMax, keyNormal;
	string valMin, valMax, valNormal;
	string getVal;
	bool flag;

	s_.OK(); ////测试正常的值
	keyNormal = GetRandomBytes_(maxKeyLen_/2 + minKeyLen_/2);
	valNormal = GetRandomBytes_(maxValLen_/2 + minValLen_/2);
	s_ = n_->Set(keyNormal, valNormal);
	EXPECT_EQ(string("OK"), s_.ToString().c_str()); 
	if(s_.ok())
	{
		s_ = n_->Get(keyNormal, &getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
		EXPECT_EQ(valNormal, getVal);
	}
	if(s_.ok())
		log_success("key长度正常，value长度正常");
	else
		log_fail("key长度正常，value长度正常");
	
	
	s_.OK(); //测试key最短的情况
	keyMin = GetRandomBytes_(minKeyLen_);
	s_ = n_->Set(keyMin, valNormal);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyMin, &getVal);
		EXPECT_EQ(valNormal, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度最短，value长度正常");
	else
		log_fail("key长度最短，value长度正常");
		
	s_.OK(); //测试val最短的值
	valMin = GetRandomBytes_(minValLen_);
	s_ = n_->Set(keyNormal, valMin);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyNormal, &getVal);
		EXPECT_EQ(valMin, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度正常， value长度最短");
	else
		log_fail("key长度正常， value长度最短");
	
	s_.OK();//测试key最长的值
	keyMax = GetRandomBytes_(maxKeyLen_);
	s_ = n_->Set(keyMax, valNormal);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyMax, &getVal);
		EXPECT_EQ(valNormal, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度最长， value长度正常");
	else
		log_fail("key长度最长， value长度正常");

	s_.OK();//测试val最长的值
	valMax = GetRandomBytes_(maxValLen_);
	s_ = n_->Set(keyNormal, valMax);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyNormal, &getVal);
		EXPECT_EQ(valMax, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度正常， value长度最长");
	else
		log_fail("key长度正常， value长度最长");

	s_.OK();//测试key最长，val最长的值
	s_ = n_->Set(keyMax, valMax);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyMax, &getVal);
		EXPECT_EQ(valMax, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度最长， value长度最长");
	else
		log_fail("key长度最长， value长度最长");

	s_.OK();//测试key最短，val最短的值
	s_ = n_->Set(keyMin, valMin);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(keyMin, &getVal);
		EXPECT_EQ(valMin, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key长度最长， value长度最长");
	else
		log_fail("key长度最长， value长度最长");

	int64_t ttl;
	string key, val;
	s_.OK();//ttl默认
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	ttl = 100;
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && ttl == -1)
		log_success("测试ttl默认是否为0");
	else
		log_fail("测试ttl默认是否为0");

	s_.OK();//ttl有值, ttl>0
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val, 2);
	CHECK_STATUS(OK);
	flag = false;
	if(s_.ok())
		flag = true;
	n_->TTL(key, &ttl);
	sleep(3);
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(NotFound);
	if(flag == true && s_.IsNotFound())
		log_success("测试ttl: ttl有值(ttl=2),是否到点过期");
	else
		log_fail("测试ttl: ttl有值(ttl=2),是否到点过期");

	s_.OK();//ttl有值，ttl<=0
	s_ = n_->Set(key, val, -1);
	CHECK_STATUS(OK);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && ttl == -1)
		log_success("测试ttl：ttl为负数(ttl=-1), 有效期是否无限");
	else
		log_fail("测试ttl：ttl为负数(ttl=-1), 有效期是否无限");
}

TEST_F(NemoKVTest, TestGet)
{
	log_message("\n========TestGet========");
	string key = GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
	string val = GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
	string getVal;

	s_.OK();//测试正常Get
	s_ = n_->Set(key, val);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(key, &getVal);
		EXPECT_EQ(val, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key存在，value不为空");
	else
		log_fail("key存在，value不为空");
	
	s_.OK(); //测试val为空
	val.clear();
	s_ = n_->Set(key, val);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(key, &getVal);
		EXPECT_EQ(val, getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
	}
	if(s_.ok())
		log_success("key存在， value为空");
	else
		log_fail("key存在， value为空");

	s_.OK();//删掉key
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
	{
		s_ = n_->Get(key, &getVal);
		EXPECT_STRNE("OK", s_.ToString().c_str());
	}
	if(s_.IsNotFound())
		log_success("key不存在");
	else
		log_fail("key不存在");
}

TEST_F(NemoKVTest, TestDel)
{
	log_message("\n========TestDel========");
	string key = GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
	string val = GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
	
	s_.OK();
	s_ = n_->Set(key, val);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(!(s_.ok()))
		return;

	s_.OK();//测试正常删除
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(!(s_.ok()))
		return;
	if(s_.ok())
		log_success("key存在");
	else
		log_fail("key存在");

	s_.OK();//测试key不存在
	s_ = n_->Del(key, &del_ret);
	EXPECT_STREQ("OK", s_.ToString().c_str());	
	if(s_.ok())
		log_success("key不存在");
	else
		log_fail("key不存在");
	
}

TEST_F(NemoKVTest, TestMSet)
{
	log_message("\n========TestMSet========");
	vector<nemo::KV> kvs;
	nemo::KV kv_temp;
	string getVal;
	bool flag1, flag2;

	s_.OK(); //测试kvs为空
	s_ = n_->MSet(kvs);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	if(s_.ok())
		log_success("测试kvs没有元素的情况");
	else
		log_fail("测试kvs没有元素的情况");

	s_.OK();//测试kvs拥有正常数量的元素
	unsigned int normalMSetNum = GetRandomUint_(minMSetNum_, maxMSetNum_);
	string key, val;
	for(unsigned int index = 0; index != normalMSetNum; index++)
	{
		GetRandomKeyValue_(key, val);
		kv_temp.key = key;
		kv_temp.val = val;
		kvs.push_back(kv_temp);
	}
	s_ = n_->MSet(kvs);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	flag1 = false;
	if(s_.ok())
		flag1 = true;
	if(!s_.ok())
		return;
	flag2 = true;;
	for(vector<nemo::KV>::iterator iter = kvs.begin(); iter != kvs.end(); iter++)
	{
		key = iter->key;
		val = iter->val;
		s_ = n_->Get(key, &getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
		if(!s_.ok())
			return;
		EXPECT_EQ(val, getVal);
		if(val != getVal)
		{
			flag2 = false;
			break;
		}
	}
	
	if(flag1 == true && flag2 == true)
		log_success("测试kvs元素的数量正常的情况：num = %d", normalMSetNum);
	else
		log_fail("测试kvs元素的数量正常的情况：num = %d", normalMSetNum);
	
	s_.OK();//测试kvs拥有最大数量的元素
	kvs.clear();
	for(unsigned int index = 0; index != maxMSetNum_; index++)
	{
		GetRandomKeyValue_(key, val);
		kv_temp.key = key;
		kv_temp.val = val;
		kvs.push_back(kv_temp);
	}
	s_ = n_->MSet(kvs);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	flag1 = false;
	if(s_.ok())
		flag1 = true;
	if(!s_.ok())
		return;
	flag2 = true;
	for(vector<nemo::KV>::iterator iter = kvs.begin(); iter != kvs.end(); iter++)
	{
		key = iter->key;
		val = iter->val;
		s_ = n_->Get(key, &getVal);
		EXPECT_STREQ("OK", s_.ToString().c_str());
		if(!(s_.ok()))
			return;
		EXPECT_EQ(val, getVal);
		if(val != getVal)
		{
			flag2 = false;
			break;
		}
	} 	
	if(flag1 == true && flag2 == true)
		log_success("测试kvs元素的数量最大的情况：num = %d", maxMSetNum_);
	else
		log_fail("测试kvs元素的数量最大的情况：num = %d", maxMSetNum_);
}

TEST_F(NemoKVTest, MDel)
{
	log_message("\n========TestMDel========");
	vector<string> keys;
	string key, val;
	int64_t deleteCount;
	s_.OK(); //测试keys为空的时候
	s_ = n_->MDel(keys, &deleteCount);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, deleteCount);
	if(!(s_.ok()))
		return;
	if(s_.ok() && deleteCount == 0)
		log_success("测试keys全都不存在的时候");
	else
		log_fail("测试keys全都不存在的时候");

	s_.OK(); //测试有keys不存在的时候
	keys.clear();
	GetRandomKeyValue_(key, val);
	s_ = n_->Set(key, val);
	keys.push_back(key);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = SetSingleNormalKeyValue_(key, val);
	keys.push_back(key);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;

    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->MDel(keys, &deleteCount);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, deleteCount);
	if(s_.ok() && deleteCount == 1)
		log_success("测试有keys存在，有keys不存在的情况");
	else
		log_fail("测试有keys存在，有keys不存在的情况");

	s_.OK();//测试key数量很大的时候
	keys.erase(keys.begin(), keys.end());
	key.erase(key.begin(), key.end());
	val.erase(val.begin(), val.end());
	uint32_t numTemp = maxMDelNum_;
	uint32_t loopNum;
	while(numTemp > 0)//这么做是因为前面好像MSet太多会死
	{	
		if(numTemp > maxMSetNum_)
			loopNum = maxMSetNum_;
		else
			loopNum = numTemp;
		s_ = SetMultiNormalKeyValue_(loopNum, NULL, &keys);
		CHECK_STATUS(OK);
		if(!(s_.ok()))
			return;
		numTemp -= loopNum;
	}
	s_ = n_->MDel(keys, &deleteCount);

	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("测试删除最大数量的keys的时候：num = %d", maxMDelNum_);
	else
		log_fail("测试删除最大数量的keys的时候：num = %d",  maxMDelNum_);
	keys.clear();		
}

TEST_F(NemoKVTest, TestIncrby)
{
	log_message("\n========TestIncrby========");
	string key, val, newVal;
	s_.OK();//测试正常情况
	key = GetRandomKey_();
	val = to_string(GetRandomUint_(0, 255));
	int64_t incrVal;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	incrVal = GetRandomUint_(0, 255);
	s_ = n_->Incrby(key, incrVal, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(atoi(val.c_str()) + incrVal, atoi(newVal.c_str()));
	if(!(s_.ok()))
		return;
	if(s_.ok() && atoi(val.c_str()) == incrVal, atoi(newVal.c_str()))
		log_success("测试正常情况：原来value=%d, incrby=%lld, newValue=%d", atoi(val.c_str()), incrVal, atoi(newVal.c_str()));
	else
		log_fail("测试正常情况：原来value=%d, incrby=%lld, newValue=%d", atoi(val.c_str()), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试key不存在的情况
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);

	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrby(key, incrVal, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(incrVal, atoi(newVal.c_str()));
	if(s_.ok() && incrVal == atoi(newVal.c_str()))
		log_success("key存在的情况：incrby=%lld, newVal=%d", incrVal, atoi(newVal.c_str()));
	else
		log_fail("key存在的情况：incrby=%lld, newVal=%d", incrVal, atoi(newVal.c_str()));

	
	s_.OK();//测试原来的val不全是数字
	newVal = "0";
	key = GetRandomKey_();
	val = string("100ABGV");
	s_= n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrby(key, incrVal, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0", newVal.c_str());
	if((!s_.ok()) && 0 == atoi(newVal.c_str()))
		log_success("val不全是数字：原来value=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val不全是数字：原来value=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试原来的数值超过限制
	newVal = "0";
	key = GetRandomKey_();
	val = to_string(LLONG_MAX) + "1";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0", newVal.c_str());
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("val值超出范围：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val值超出范围：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试所加的值超过限制 
	newVal = "0";
	key = GetRandomKey_();
	val = string("-") + to_string(GetRandomUint_(0, 256));
	incrVal = LLONG_MIN;
	s_= n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0", newVal.c_str());	
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("incrby超出范围：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("incrby超出范围：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试只有一个加号的情况
	newVal = "0";
	key = GetRandomKey_();
	val = string("+");
	incrVal = 1;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	//CHECK_STATUS(OK);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0", newVal.c_str());
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("val只有一个加号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val只有一个加号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试只有一个减号的情况
	newVal = "0";
	key = GetRandomKey_();
	val = string("-");
	incrVal = 1;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	STATUS_NOT(OK);
	EXPECT_STREQ("0", newVal.c_str());
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("val只有一个减号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val只有一个减号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));


	s_.OK();//测试val是多个0的时候
	newVal = "0";
	key = GetRandomKey_();
	val = string("0000");
	incrVal = 4;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	CHECK_STATUS(OK);
	EXPECT_STREQ("4", newVal.c_str());
	if(s_.ok() && 4 == atoi(newVal.c_str()))
		log_success("val是多个0的情况：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val是多个0的情况：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));

	s_.OK();//测试有加号的情况	
	newVal = "0";
	key = GetRandomKey_();
	val = string("+10");
	incrVal = 2;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Incrby(key, incrVal, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(12, atoi(newVal.c_str()));
	if(s_.ok() && 12 == atoi(newVal.c_str()))
		log_success("val有加号的情况： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
	else
		log_fail("val有加号的情况： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), incrVal, atoi(newVal.c_str()));
}

TEST_F(NemoKVTest, TestDecrby)
{
	log_message("\n========TestDecrby========");
	string key, val, newVal;
	int64_t decrby;
	s_.OK(); //测试正常情况
	key = GetRandomKey_();
	val = string("-") + to_string(GetRandomUint_(0, 256));
	decrby = GetRandomUint_(0, 256);
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Decrby(key, decrby, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(atoi(val.c_str())-decrby, atoi(newVal.c_str()));
	if(s_.ok() && atoi(val.c_str())-decrby == atoi(newVal.c_str()))	
		log_success("测试正常情况：原来value=%s, decrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("测试正常情况：原来value=%s, decrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));

	s_.OK();//测试没有key的情况下
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Decrby(key, decrby, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(0-decrby, atoi(newVal.c_str()));
	if(s_.ok() && 0-decrby == atoi(newVal.c_str()))
		log_success("key不存在的情况：原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("key不存在的情况：原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	
	s_.OK();//测试val不全是数字的情况
	key = GetRandomKey_();
	val = "100ABGV";
	newVal = "0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_EQ(string("0"), newVal);
	if((!s_.ok()) && 0 == atoi(newVal.c_str()))
		log_success("val不全是数字： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("val不全是数字： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));

	s_.OK();//测试原来的val是超范围的
	key = GetRandomKey_();
	val = string("-") + to_string(LLONG_MAX);
	newVal = "0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, 2, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_EQ(string("0"), newVal);
	if((!s_.ok()) && 0 == atoi(newVal.c_str()))
		log_success("原来val超范围： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("原来val超范围： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	
	s_.OK();//测试减小的值超范围
	key = GetRandomKey_();
	val = string("-100");
	decrby = LLONG_MAX;
	newVal = "0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_EQ(string("0"), newVal);
	if((!s_.ok()) && 0 == atoi(newVal.c_str()))
		log_success("decrby超出范围： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("decrby超出范围： 原来value=%s, decrby=%lld, newVal=%d", val.c_str(), decrby, atoi(newVal.c_str()));

	s_.OK();//测试只有一个加号的情况
	newVal = "0";
	key = GetRandomKey_();
	val = string("+");
	decrby = 1;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	//CHECK_STATUS(OK);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0", newVal.c_str());
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("val只有一个加号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("val只有一个加号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));

	s_.OK();//测试只有一个减号的情况
	newVal = "0";
	key = GetRandomKey_();
	val = string("-");
	decrby = 1;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	STATUS_NOT(OK);
	EXPECT_STREQ("0", newVal.c_str());
	if((!(s_.ok())) && 0 == atoi(newVal.c_str()))
		log_success("val只有一个减号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("val只有一个减号： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));


	s_.OK();//测试val是多个0的时候
	newVal = "0";
	key = GetRandomKey_();
	val = string("0000");
	decrby = 4;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	CHECK_STATUS(OK);
	EXPECT_STREQ("-4", newVal.c_str());
	if(s_.ok() && -4 == atoi(newVal.c_str()))
		log_success("val是多个0的情况：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("val是多个0的情况：原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));

	s_.OK();//测试有加号的情况	
	newVal = "0";
	key = GetRandomKey_();
	val = string("+10");
	decrby = 2;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	s_ = n_->Decrby(key, decrby, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(8, atoi(newVal.c_str()));
	if(s_.ok() && 8 == atoi(newVal.c_str()))
		log_success("val有加号的情况： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
	else
		log_fail("val有加号的情况： 原来val=%s, incrby=%lld, newValue=%d", val.c_str(), decrby, atoi(newVal.c_str()));
}

TEST_F(NemoKVTest, TestIncrbyfloat)
{
	log_message("\n========TestIncrbyfloat========");
	string key, val, newVal;
	double incrbyDouble, diffDouble;
	s_.OK();//测试正常情况
	key = GetRandomKey_();
	val = to_string(12.02);
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	incrbyDouble = 31.0;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	//EXPECT_EQ(atof(val.c_str()) + incrbyDouble, atof(newVal.c_str()));
	diffDouble = atof(val.c_str()) + incrbyDouble - atof(newVal.c_str());
	EXPECT_EQ(true, diffDouble > -eps && diffDouble < eps);
	if(s_.ok() && diffDouble > -eps && diffDouble < eps)
		log_success("测试正常情况：原来value=%f, incrbyDouble=%lf, newValue=%lf", atof(val.c_str()), incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("测试正常情况：原来value=%f, incrbyDouble=%lf, newValue=%lf", atof(val.c_str()), incrbyDouble, atof(newVal.c_str()));

	s_.OK();//测试key不存在的情况
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	//EXPECT_EQ(incrbyDouble, atof(newVal.c_str()));
	diffDouble = incrbyDouble - atof(newVal.c_str());
	EXPECT_EQ(true, diffDouble > -eps && diffDouble < eps);
	if(s_.ok() && diffDouble > -eps && diffDouble < eps)
		log_success("key不存在的情况：incrbyDouble=%lf, newValue=%lf", incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("key不存在的情况：incrbyDouble=%lf, newValue=%lf", incrbyDouble, atof(newVal.c_str()));

	s_.OK();//原val有非数字字符
	val = "12.0dfm";
	newVal = "0.0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	//EXPECT_EQ(0, atof(newVal.c_str()));
	EXPECT_EQ(true, atof(newVal.c_str()) > -eps && atof(newVal.c_str()) < eps);
	if((!s_.ok()) && atof(newVal.c_str()) > -eps && atof(newVal.c_str()) < eps)
		log_success("有非数字的val： 原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("有非数字的val： 原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));

	s_.OK();//原val带加号
	val = "+12.43";
	newVal = "0.0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	CHECK_STATUS(OK);
	//EXPECT_EQ(atof(val.c_str())+incrbyDouble, atof(newVal.c_str()));
	diffDouble = atof(val.c_str())+incrbyDouble - atof(newVal.c_str());
	EXPECT_EQ(true, diffDouble > -eps && diffDouble < eps);
	if(s_.ok() && diffDouble > -eps && diffDouble < eps)
		log_success("原val带加号：原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("原val带加号：原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));

	s_.OK();//原val带加减号
	val = "-2.23";
	newVal = "0.0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	CHECK_STATUS(OK);
	//EXPECT_EQ(atof(val.c_str())+incrbyDouble, atof(newVal.c_str()));
	diffDouble = atof(val.c_str())+incrbyDouble - atof(newVal.c_str());
	EXPECT_EQ(true, diffDouble > -eps && diffDouble < eps);
	if((s_.ok()) && diffDouble > -eps && diffDouble < eps)
		log_success("原val带减号：原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("原val带减号：原来value=%s, incrbyDouble=%lf, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));

	s_.OK();//测试所加数字无限大
	val = "12.01";
	newVal = "0.0";
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	incrbyDouble = 1.0/0;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	EXPECT_STREQ("0.0", newVal.c_str());
	//EXPECT_EQ(true, newVal.c_str() > -eps && newVal.c_str() < eps);
	if((!s_.ok()) && string("0.0") == newVal)
		log_success("所加数字无限大：原来value=%s, incrbyDouble=1.0/0, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));
	else
		log_fail("所加数字无限大：原来value=%s, incrbyDouble=1.0/0, newValue=%lf", val.c_str(), incrbyDouble, atof(newVal.c_str()));

	s_.OK();//测试结果的小数为0
	val = "7.55";
	incrbyDouble = 2.45;
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Incrbyfloat(key, incrbyDouble, newVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	EXPECT_EQ(string("10"), newVal);
	if(s_.ok() && string("10") == newVal)
		log_success("结果小数位全为0：原来value=%s, incrbyDouble=%lf, newValue=%s", val.c_str(), incrbyDouble, newVal.c_str());
	else
		log_fail("结果小数位全为0：原来value=%s, incrbyDouble=%lf, newValue=%s", val.c_str(), incrbyDouble, newVal.c_str());
	
}

TEST_F(NemoKVTest, TestGetSet)
{
	log_message("\n========TestGetSet========");
	string key, val, oldVal, newVal;
	
	s_.OK();//测试正常的情况
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	newVal = GetRandomVal_();
	s_ = n_->GetSet(key, newVal, &oldVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	EXPECT_EQ(val, oldVal);
	if(s_.ok() && val == oldVal)
		log_success("测试原来key存在的情况");
	else
		log_fail("测试原来key存在情况");

	s_.OK();//测试key不存在
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	s_ = n_->GetSet(key, newVal, &oldVal);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	EXPECT_EQ(string(""), oldVal); //检测和原来的值为空。
	if(s_.ok() && string("") == oldVal)
		log_success("测试原来key不存在情况");
	else
		log_fail("测试原来key不存在情况");
}

TEST_F(NemoKVTest, TestAppend)
{
	log_message("\n========TestAppend========");
	string key, val, appendVal, retVal;
	int64_t appendLen;

	s_.OK();//测试正常情况
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	appendVal = GetRandomVal_();
	s_ = n_->Append(key, appendVal, &appendLen);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	EXPECT_EQ(val+appendVal, retVal);
	if(s_.ok() && val+appendVal == retVal)
		log_success("正常情况：key存在");
	else
		log_fail("正常情况：key存在");

	s_.OK();//测试key不存在
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Append(key, appendVal, &appendLen);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	EXPECT_EQ(appendVal, retVal);
	if(s_.ok() && retVal == appendVal)
		log_success("key不存在");
	else
		log_fail("key不存在");
}

TEST_F(NemoKVTest, TestSetnx)
{
	log_message("\n========TestSetnx========");
	string key, val, newVal, retVal;
	int64_t ret;
	s_.OK();//测试原来的key存在
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	newVal = GetRandomVal_();
	s_ = n_->Setnx(key, newVal, &ret);
	EXPECT_STREQ("OK", s_.ToString().c_str());
	EXPECT_EQ(0, ret);
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val, retVal);
	if(s_.ok() && val == retVal && ret == 0)
		log_success("测试原来的key存在");
	else
		log_fail("测试原来的key存在");

	s_.OK();//测试原来的key不存在
	newVal = GetRandomVal_();
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	newVal = GetRandomVal_();
	s_ = n_->Setnx(key, newVal, &ret);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	EXPECT_EQ(newVal, retVal);
	if(s_.ok() && newVal == retVal && ret == 1)
		log_success("测试原来的key不存在");
	else
		log_fail("测试原来的key不存在");
	
	int64_t ttl;
	int64_t res;
	string getVal;
	s_.OK();//ttl默认值
	res = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Del(key, &del_ret);
	s_ = n_->Setnx(key, val, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && res == 1 && ttl == -1)
		log_success("测试ttl默认值：有效期是否无限");
	else
		log_fail("测试ttl默认值：有效期是否无限");

	s_.OK();//ttl有值， ttl>0
	res = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Del(key, &del_ret);
	s_ = n_->Setnx(key, val, &res, 2);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	sleep(3);
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(NotFound);
	if(res == 1 && s_.IsNotFound())
		log_success("测试ttl>0：ttl=2，是否按时过期");
	else
		log_fail("测试ttl>0：ttl=2，是否按时过期");
		

	s_.OK();//ttl<=0
	res = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Del(key, &del_ret);
	s_ = n_->Setnx(key, val, &res, -1);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(res == 1 && s_.ok() && ttl == -1)
		log_success("测试ttl<=0：ttl=-1，有效期是否无限");
	else
		log_fail("测试ttl<=0：ttl=-1，有效期是否无限");
}

TEST_F(NemoKVTest, TestSetxx)
{
	log_message("\n========TestSetxx========");
	string key, val, newVal, retVal;
	int64_t ret;

	s_.OK();//测试正常情况，即原key存在的时候
	s_ = SetSingleNormalKeyValue_(key, val);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	newVal = GetRandomVal_();
	s_ = n_->Setxx(key, newVal, &ret);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	if(!(s_.ok()))
		return;
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(newVal, retVal);
	if(s_.ok() && retVal == newVal && ret == 1)
		log_success("测试原来的key存在的情况");
	else
		log_fail("测试原来的key存在的情况");

	s_.OK();
    int64_t del_ret;
	s_ = n_->Del(key, &del_ret);
	CHECK_STATUS(OK);
	if(!(s_.ok()))
		return;
	newVal = GetRandomVal_();
	s_ = n_->Setxx(key, newVal, &ret);
	EXPECT_EQ(0, ret);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	s_ = n_->Get(key, &retVal);
	EXPECT_STRNE("OK", s_.ToString().c_str());
	if(s_.IsNotFound() && ret == 0)
		log_success("测试原来的key不存在的情况");
	else
		log_fail("测试原来的key不存在的情况");

	int64_t ttl;
	s_.OK();//默认值
	ret = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	s_ = n_->Setxx(key, val, &ret);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && ret == 1 && ttl == -1)
		log_success("测试ttl默认值：有效期是否无限");
	else
		log_fail("测试ttl默认值：有效期是否无限");

	s_.OK();//ttl>0
	ret = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	s_ = n_->Setxx(key, val, &ret, 2);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	sleep(3);
	s_ = n_->Get(key, &retVal);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound() && ret == 1)
		log_success("测试ttl>0：ttl=2，是否按时过期");
	else
		log_fail("测试ttl>0：ttl=2，是否按时过期");


	s_.OK();//ttl<=0
	ret = 0;
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	s_ = n_->Setxx(key, val, &ret, -1);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && ret == 1 && ttl == -1)
		log_success("测试ttl<=0：ttl=-1，有效期是否无限");
	else
		log_fail("测试ttl<=0：ttl=-1，有效期是否无限");
}

//开始写的简单点

TEST_F(NemoKVTest, TestMSetnx)
{
	log_message("\n========TestMSetnx========");
	vector<nemo::KV> kvs;
	nemo::KV kvTemp;
	string key, val;
	int64_t del_ret, ret, arbiter;

	s_.OK();//全部的key都不存在
	uint32_t loopNum = GetRandomUint_(2, maxMSetNum_);
	for(uint32_t index = 0; index != loopNum; index++)
	{
		key = GetRandomKey_();
		val = GetRandomVal_();
		kvTemp.key = key;
		kvTemp.val = val;
		kvs.push_back(kvTemp);
	}
	for(vector<nemo::KV>::iterator iter = kvs.begin(); iter != kvs.end(); iter++)
	{
		n_->Del(iter->key, &del_ret);
	}
	s_ = n_->MSetnx(kvs, &ret);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, ret);
	if(s_.ok() && ret == 1)
		log_success("测试全部的key都不存在的情况");
	else
		log_fail("测试全部的key都不存在的情况");
	
	s_.OK();//有key存在
	for(vector<nemo::KV>::iterator iter = kvs.begin(); iter != kvs.end(); iter++)
	{
		n_->Del(iter->key, &del_ret);
	}
	arbiter = GetRandomUint_(0, loopNum-1);
	vector<nemo::KV>::iterator iter = kvs.begin();
	uint32_t index = 0;
	for(vector<nemo::KV>::iterator iter = kvs.begin(); iter != kvs.end(); iter++)
	{
		if(index == arbiter)
		{
			s_ = n_->Set(iter->key, iter->val);
			CHECK_STATUS(OK);
			break;
		}
		index++;
	}
	s_ = n_->MSetnx(kvs, &ret);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, ret);
	if(s_.ok() && ret == 0)
		log_success("测试有key存在的情况");
	else
		log_fail("测试有key存在的情况");

	kvs.clear();
}

TEST_F(NemoKVTest, TestGetrange)
{
	log_message("\n========TestGetrange========");
	string key, val, getVal, stateStr;
    int64_t del_ret;
	int64_t start_t, end_t;
	s_.OK();//测试正常情况，0<=start_t <= end_t < size
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	start_t = GetRandomUint_(0, val.length());
	end_t = GetRandomUint_(start_t, val.length());
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val.substr(start_t, end_t-start_t+1), getVal);
	if(s_.ok() && val.substr(start_t, end_t-start_t+1) == getVal)
		log_success("0<=start_t <= end_t < size的情况");
	else
		log_fail("0<=start_t <= end_t < size的情况");

	s_.OK();//key不存在
	getVal = "";
	n_->Del(key, &del_ret);
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, getVal.empty());
	if(s_.IsNotFound() && getVal.empty())
		log_success("原来key不存在");
	else
		log_fail("原来key不存在");

	s_.OK();//val为空
	val = "";
	getVal = "mm";
	n_->Set(key, val);
	start_t = -1;
	end_t = 2;
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val,getVal);
	if(s_.ok() && val == getVal)
		log_success("val为空");
	else
		log_fail("val为空");


	s_.OK();//start_t<0;end_t>0
	val = GetRandomVal_();
	n_->Set(key, val);
	start_t = GetRandomUint_(0, val.length());
	end_t = GetRandomUint_(start_t, val.length());
	start_t = start_t-val.length();
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val.substr(val.length()+start_t, end_t-start_t-val.length()+1), getVal);
	if(s_.ok() && val.substr(val.length()+start_t, end_t-val.length()-start_t+1) == getVal)
		log_success("start_t<=0;end_t正常取值的情况");
	else
		log_fail("start_t<=0;end_t正常的情况");


	s_.OK();//start_t>0; end_t<0
	start_t = GetRandomUint_(0, val.length());
	end_t = GetRandomUint_(start_t, val.length());
	end_t = end_t - val.length();
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val.substr(start_t, val.size()+end_t-start_t+1), getVal);
	if(s_.ok() && val.substr(start_t, val.size()+end_t-start_t+1) == getVal)
		log_success("start_t正常取值; end_t<=0的情况");
	else
		log_fail("start_t正常取值; end_t<=0的情况");

	s_.OK();//start_t < 0; end_t >= size
	//start_t = (-1)*GetRandomUint_(1, val.length());
	start_t = GetRandomUint_(1, val.length());
	start_t = start_t - val.length();
	end_t = val.length() + 100;
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val.substr(start_t + val.length()), getVal);
	if(s_.ok() && val.substr(start_t + val.length()) == getVal)
		log_success("start_t<=0; end_t >= 字符串长度的情况");
	else
		log_fail("start_t<=0; end_t >= 字符串长度的情况");

	s_.OK();//0 < start_t <size; end_t >=size
	start_t = GetRandomUint_(0, val.length());
	end_t = val.length() + 100;
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(val.substr(start_t, val.length()-start_t), getVal);
	if(s_.ok() && val.substr(start_t, val.length()-start_t) == getVal)
		log_success("start_t正常取值; end_t >=字符串长度");
	else
		log_fail("start_t正常取值; end_t >=字符串长度");

	s_.OK();//start_t>=size; 0<=end_t<=size 
	getVal = "";
	start_t = val.length() + 100;
	end_t = GetRandomUint_(0, val.length());
	s_ = n_->Getrange(key, start_t, end_t, getVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, getVal.empty());
	if(s_.ok() && getVal.empty())
		log_success("start_t>=字符串长度， end_t正常取值");
	else
		log_fail("start_t>=字符串长度， end_t正常取值");
}

TEST_F(NemoKVTest, TestSetrange)
{
	log_message("\n========TestSetrange========");
	string key, val, insertVal, getVal, newVal;
	int64_t offset, newLen, insertValLen;

	s_.OK();//key存在;0<=offset<val.size();0<=offset+value.size() <=val.size();
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	offset = GetRandomUint_(0, val.length()-1);
	insertValLen = GetRandomUint_(0, val.length()-offset);
	insertVal = GetRandomBytes_(insertValLen);
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(OK);
	EXPECT_EQ((int64_t)(val.length()), newLen);
	newVal = val.substr(0, offset);
	newVal = newVal.append(insertVal);
	newVal.append(val.substr(offset+insertValLen));
	n_->Get(key, &getVal);
	EXPECT_EQ(newVal, getVal);
	if(s_.ok() && newVal == getVal)
		log_success("key存在;0<=offset<原来字符串长度;0<=offset+插入字符串长度 <=原字符串长度");
	else
		log_fail("key存在;0<=offset<原来字符串长度;0<=offset+插入字符串长度 <=原字符串长度");

	s_.OK();//0<=offset<val.size();val.size()<offset+value.size();
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	offset = GetRandomUint_(0, val.length()-1);
	insertValLen = GetRandomUint_(val.length()-offset+2, val.length()-offset+100);
	insertVal = GetRandomBytes_(insertValLen);
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(OK);
	EXPECT_EQ(offset+insertValLen, newLen);
	newVal = val.substr(0, offset);
	newVal = newVal + insertVal;
	getVal.clear();
	n_->Get(key, &getVal);
	EXPECT_EQ(newVal, getVal);
	if(s_.ok() && newVal == getVal)
		log_success("0<=offset<原来字符串长度;原来字符串长度<offset+插入字符串长度");
	else
		log_fail("0<=offset<原来字符串长度;原来字符串长度<offset+插入字符串长度");

	s_.OK();//offset > val.size()
	offset = GetRandomUint_(val.length(), val.length()+100);
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(OK);
	EXPECT_EQ(offset+insertValLen, newLen);
	if(s_.ok() && offset+ insertValLen == newLen)
		log_success("offset > 原来字符串长度");
	else
		log_fail("offset > 原来字符串长度");

	s_.OK();//offset + value.size()>512M
	newLen = 0;
	offset = 1<<29;
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(0, newLen);
	if(s_.IsCorruption() && newLen == 0)
		log_success("offset + 插入字符串长度>512M");
	else
		log_fail("offset + 插入字符串长度>512M");

	s_.OK();//offset < 0;
	newLen = 0;
	offset = -GetRandomUint_(0, val.length()-1);
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(0, newLen);
	if(s_.IsCorruption() && newLen == 0)
		log_success("offset < 0");
	else
		log_fail("offset < 0");

	s_.OK();//原来key不存在
	offset = GetRandomUint_(0, val.length());
	insertValLen = GetRandomUint_(0, val.length()-offset);
	insertVal = GetRandomBytes_(insertValLen);

    int64_t del_ret;
	n_->Del(key, &del_ret);
	s_ = n_->Setrange(key, offset, insertVal, &newLen);
	CHECK_STATUS(OK);
	n_->Get(key, &getVal);
	newVal = string(offset, '\0');
	newVal.append(insertVal);
	EXPECT_EQ(newVal, getVal);
	if(s_.ok() && newVal == getVal)
		log_success("原来key不存在");
	else
		log_fail("原来key不存在");
}

TEST_F(NemoKVTest, TestStrlen)
{
	log_message("\n========TestStrlen========");
	string key, val;
	int64_t len;
	s_.OK();//key存在
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	s_ = n_->Strlen(key, &len);
	CHECK_STATUS(OK);
	EXPECT_EQ((int64_t)(val.length()), len);
	if(s_.ok() && (int64_t)(val.length()) == len)
		log_success("原来的key存在");
	else
		log_fail("原来的key存在");

	s_.OK();//key不存在
	len = 0;

    int64_t del_ret;
	n_->Del(key, &del_ret);
	s_ = n_->Strlen(key, &len);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, len);
	if(s_.IsNotFound() && len == 0)
		log_success("原来的key不存在");
	else
		log_fail("原来的key不存在");
}

TEST_F(NemoKVTest, TestScan)
{
	log_message("\n========TestScan========");
	string start, end;
	size_t index, startInt, endInt;
	nemo::KIterator *kIterPtr;

	/*
	start="";
	end="";
	kIterPtr = n_->KScan(start, end, -1);
	while(kIterPtr->Next())
	{
		n_->Del(kIterPtr->key());
	}
	*/

	bool flag1, flag2, flag3;
	string key, val;
	string keyPre = "nemo_scan_test";
	int64_t totalKeyNum = 3;
	int64_t numPre = 10000;
	for(int64_t index = 0; index < totalKeyNum; index++)
	{
		key = keyPre + itoa(numPre + index);
		val = GetRandomVal_();
		n_->Set(key, val);
	}

	log_message("========keys from %s%lld to %s%lld========", keyPre.c_str(), numPre+0, keyPre.c_str(), numPre + totalKeyNum-1);

	s_.OK();//start和end都在给定keys范围之内,limit=-1(这里测试头尾)
	startInt = 0;
	start=string("nemo_scan_test") + itoa(numPre + 0);
	endInt = totalKeyNum - 1;
	end = string("nemo_scan_test") + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, -1);
	flag1 = false; flag2 = false; flag3 = false;
	EXPECT_EQ(true, kIterPtr->Valid());

  //Note: index should start at -1
	index = 0;
	EXPECT_EQ(string("nemo_scan_test") + itoa(numPre+index), kIterPtr->key());
	if(keyPre + itoa(numPre+index) == kIterPtr->key())
		flag1 = true;
  for (; kIterPtr->Valid(); kIterPtr->Next())
	{
		//EXPECT_EQ(string("nemo_scan_test") + itoa(numPre + index), kIterPtr->key());
		index++;
	}
	EXPECT_EQ(string("nemo_scan_test") + itoa(numPre+totalKeyNum-1), kIterPtr->key());
	if(keyPre + itoa(numPre+totalKeyNum-1) == kIterPtr->key())
		flag2 = true;
	EXPECT_EQ(totalKeyNum, index);
	if(index == totalKeyNum)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("start和end都在keys范围内：start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	else
		log_fail("start和end都在keys范围内：start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	delete kIterPtr;
	
	s_.OK();//start和end都不在在keys的内部（这里测试刚好头尾外）
	flag1 = false; flag2 = false; flag3 = false;
	startInt = 0;
	start = string("nemo_scan_test");
	endInt = totalKeyNum;
	end = string("nemo_scan_test") + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, -1);
	index=0;
	EXPECT_EQ(true, kIterPtr->Valid());
	EXPECT_EQ(string("nemo_scan_test") + itoa(numPre+index), kIterPtr->key());
	if(string("nemo_scan_test") + itoa(numPre+index) == kIterPtr->key())
		flag1 = true;
  for (; kIterPtr->Valid(); kIterPtr->Next())
	{
		index++;
		//EXPECT_EQ(string("nomo_scan_test") + itoa(numPre+index), kIterPtr->key());
	}
	EXPECT_EQ(string("nemo_scan_test") + itoa(numPre+totalKeyNum-1), kIterPtr->key());
	if(string("nemo_scan_test") + itoa(numPre+totalKeyNum-1) == kIterPtr->key())
		flag2 = true;
	EXPECT_EQ(totalKeyNum, index);
	if(index == totalKeyNum)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("start和end都不在keys范围内，但是包括住keys：start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	else
		log_fail("start和end都不在keys范围内，但是包括住keys：start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	delete kIterPtr;
	
	s_.OK();//start在keys的内部， end不在keys内部
	flag1 = false; flag2 = false; flag3 = false;
	startInt = GetRandomUint_(0, totalKeyNum-1);
	endInt = totalKeyNum + 100;
	start = string("nemo_scan_test") + itoa(numPre + startInt);
	end = keyPre + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, -1);
	index = startInt;
	EXPECT_EQ(true, kIterPtr->Valid());
	EXPECT_EQ(keyPre + itoa(numPre + index), kIterPtr->key());
	if(keyPre + itoa(numPre + index) == kIterPtr->key())
		flag1 = true;
  for (; kIterPtr->Valid(); kIterPtr->Next())
	{
		index++;
		//EXPECT_EQ(keyPre + itoa(numPre + index), kIterPtr->key());
	}
	EXPECT_EQ(keyPre + itoa(numPre + totalKeyNum - 1), kIterPtr->key());
	if(keyPre + itoa(numPre + totalKeyNum - 1) == kIterPtr->key())
		flag2 = true;
	EXPECT_EQ(totalKeyNum, index);
	if(index == totalKeyNum)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("start在keys范围内，end在keys范围外： start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	else
		log_fail("start在keys范围内，end在keys范围外： start=%s, end=%s, limit=-1", start.c_str(), end.c_str());
	delete kIterPtr;

	s_.OK();//start不在keys的内部，end在keys内部
	flag1 = false; flag2 = false; flag3 = false;
	startInt = 0;
	start = keyPre;
	endInt = GetRandomUint_(0, totalKeyNum-1);
	end = keyPre + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, -1);
	index = startInt;
	EXPECT_EQ(true, kIterPtr->Valid());
	EXPECT_EQ(keyPre + itoa(numPre + index), kIterPtr->key());
	if(keyPre + itoa(numPre + index) == kIterPtr->key())
		flag1 = true;
  for (; kIterPtr->Valid(); kIterPtr->Next())
	{
		index++;
	}
	EXPECT_EQ(keyPre + itoa(numPre + endInt), kIterPtr->key());
	if(keyPre + itoa(numPre + endInt) == kIterPtr->key())
		flag2 = true;
	EXPECT_EQ(endInt, index - 1);
	if(endInt == index)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("start在keys外，end在keys内部： start=%s, end=%s, limits=-1", start.c_str(), end.c_str());
	else
		log_fail("start在keys外，end在keys内部： start=%s, end=%s, limits=-1", start.c_str(), end.c_str());
	delete kIterPtr;

	s_.OK(); //start，end都在keys的外面，而且它们和keys没有交集
	flag1 = false; flag2 = false; flag3 = false; 
	startInt = totalKeyNum + 10;
	endInt = totalKeyNum + 20;
	start = keyPre + itoa(numPre + startInt);
	end = keyPre + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, -1);
  for (; kIterPtr->Valid(); kIterPtr->Next());
	EXPECT_EQ(true, (kIterPtr->key()).empty());
	if(true == (kIterPtr->key()).empty())
		log_success("start，end都在keys外，且和keys没有交集： start=%s, end=%s, limits=-1", start.c_str(), end.c_str());
	else
		log_fail("start，end都在keys外，且和keys没有交集： start=%s, end=%s, limits=-1", start.c_str(), end.c_str());
	delete kIterPtr;

	/*
	s_.OK();//start和end都为空
	flag1 = false; flag2 = false;
	startInt = 0;
	endInt = 0;
	start = "";
	end = "";
	kIterPtr = n_->KScan(start, end, -1);
	index = -1;
	while(kIterPtr->Next())
	{
		index++;
	}
	EXPECT_EQ(keyPre + itoa(numPre + totalKeyNum-1), kIterPtr->key());
	if(keyPre + itoa(numPre + totalKeyNum-1) == kIterPtr->key())
		flag1 = true;
	EXPECT_EQ(totalKeyNum-1, index);
	if(totalKeyNum-1 == index)
		flag2 = true;
	if(flag1 && flag2)
		log_success("start和end都为空, limits = -1");
	else
		log_fail("start和end都为空, limits = -1");
	*/
	
	s_.OK();//测试limit的作用
	flag1 = false; flag2 = false; flag3 = false;
	startInt = 0;
	endInt = totalKeyNum;
	uint64_t limit = GetRandomUint_(1, totalKeyNum-2);
	start = keyPre + itoa(numPre + startInt);
	end = keyPre + itoa(numPre + endInt);
	kIterPtr = n_->KScan(start, end, limit);
	index = 0;
	EXPECT_EQ(true, kIterPtr->Valid());
	EXPECT_EQ(keyPre + itoa(numPre + index), kIterPtr->key());
	if(keyPre + itoa(numPre + index) == kIterPtr->key())
		flag1 = true;
  for (; kIterPtr->Valid(); kIterPtr->Next())
	//while(kIterPtr->Next())
	{
		index++;
	}
	EXPECT_EQ(keyPre + itoa(numPre + limit -1 ), kIterPtr->key());
	if(keyPre + itoa(numPre + limit -1 ) == kIterPtr->key())
		flag2 = true;
	EXPECT_EQ(limit, index);
	if(limit-1 == index)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("测试limit： limit=%lld", limit);
	else
		log_fail("测试limit： limit=%lld", limit);
	delete kIterPtr;
}

TEST_F(NemoKVTest, TestTTL)
{
	log_message("\n========TestTTL========");
	string key, val;
	int64_t res;

	s_.OK(); //原始写入为0
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val, 0);
	s_ = n_->TTL(key, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(-1, res);
	if(s_.ok() && res == -1)
		log_success("key存在，且是持久的，res=%lld", res);
	else
		log_fail("key存在，且是持久的，res=%lld", res);

	s_.OK();//key存在；ttl!=0
	n_->Set(key, val, 10);
	sleep(3);
	s_ = n_->TTL(key, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, 6<=res&&res<=8);
	if(s_.ok() && res <= 8 && res >=6)
		log_success("key存在；ttl!=0: ttl=%d, res=%lld", 10, res);
	else
		log_fail("key存在；ttl!=0: ttl=%d, res=%lld", 10, res);

	s_.OK();//key不存在
    int64_t del_ret;
	n_->Del(key, &del_ret);
	s_ = n_->TTL(key, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(-2, res);
	if(s_.IsNotFound() && res == -2)
		log_success("原来的key不存在");
	else
		log_fail("原来的key不存在");
}

TEST_F(NemoKVTest, TestPersist)
{
	log_message("\n========TestPersist========");
	string key, val;
	int64_t res, ttl;
	s_.OK();//key存在；ttl=0
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val, 0);
	s_ = n_->Persist(key, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && res == 0 && ttl == -1)
		log_success("key存在，原来是持久的： ttl=%lld", ttl);
	else
		log_fail("key存在，原来是持久的： ttl=%lld", ttl);

	s_.OK();//key存在；ttl不为0，未过期
	n_->Set(key, val, 100);
	s_ = n_->Persist(key, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(s_.ok() && res == 1 && ttl == -1)
		log_success("key存在，不持久且未过期， ttl=%lld", ttl);
	else
		log_fail("key存在，不持久且未过期， ttl=%lld", ttl);

	s_.OK();//key不存在
    int64_t del_ret;
	n_->Del(key, &del_ret);
	s_ = n_->Persist(key, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if(s_.IsNotFound() && res == 0)
		log_success("key不存在");
	else
		log_fail("key不存在");
}

TEST_F(NemoKVTest, TestExpire)
{
	log_message("\n========TestExpire========");
	string key, val;
	int64_t res, ttl;

	s_.OK();//key存在；seconds>0
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val, 100);
	s_ = n_->Expire(key, 50, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->TTL(key, &ttl);
	EXPECT_EQ(true, 45<=ttl && ttl <= 50);
	if(s_.ok() && res == 1 && 45<=ttl && ttl <= 50)	
		log_success("key存在, seconds>0：seconds=50, ttl=%lld", ttl);
	else
		log_fail("key存在, seconds>0：seconds=50, ttl=%lld", ttl);

	s_.OK();//key存在；seconds<=0;
	s_ = n_->Expire(key, -1, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	s_ = n_->Get(key, &val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound() && res == 1)
		log_success("key存在，seconds<=0： seconds=-1");
	else
		log_fail("key存在，seconds<=0： seconds=-1");
	
	s_.OK();//key不存在
    int64_t del_ret;
	n_->Del(key, &del_ret);
	s_ = n_->Expire(key, 10, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if(res == 0 && s_.IsNotFound())
		log_success("key不存在情况");
	else
		log_fail("key不存在情况");
}


TEST_F(NemoKVTest, TestExpireat)
{
	log_message("\n========TestExpireat========");
	string key, val, getVal;
	int32_t timestamp;
	int64_t res;
	bool flag1, flag2, flag3;

	s_.OK();//key存在，timestamp>当前时间
	flag1 = false; flag2 = false;
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	timestamp = time(NULL) + 1;
	s_ = n_->Expireat(key, timestamp, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if(s_.ok() && res == 1)
		flag1 = true;
	s_ = n_->Get(key, &getVal);
	EXPECT_EQ(val, getVal);
	sleep(2);
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		flag2 = true;
	if(flag1 && flag2)
		log_success("key存在，timestamp>当前时间，是否按时过期");
	else
		log_fail("key存在，timestamp>当前时间，是否按时过期");
	

	s_.OK();//key存在，timestamp<当前时间
	flag1 = false; flag2 = false;
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->Set(key, val);
	n_->Get(key, &getVal);
	EXPECT_EQ(val, getVal);
	timestamp = time(NULL) - 1;
	s_ = n_->Expireat(key, timestamp, &res);
	CHECK_STATUS(OK);
	if(s_.ok())
		flag1 = true;
	EXPECT_EQ(1, res);
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		flag2 = true;
	if(flag1 && flag2)
		log_success("key存在，timestamp<当前时间,是否删除成功");
	else
		log_fail("key存在，timestamp<当前时间,是否删除成功");

	s_.OK();//key不存在
    int64_t del_ret;
	n_->Del(key, &del_ret);
	timestamp = time(NULL);
	s_ = n_->Expireat(key, timestamp, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if(s_.IsNotFound() && res == 0)
		log_success("key不存在");
	else
		log_fail("key不存在");
}

TEST_F(NemoKVTest, TestSetWithExpireAt)
{
	string key, val, getVal;
	int32_t timestamp;
	int64_t ttl;
	bool flag1, flag2, flag3;

	s_.OK();//timestamp>当前时间
	flag1 = false; flag2 = false; flag3 = false;
	key = GetRandomKey_();
	val = GetRandomVal_();
	timestamp = time(NULL) + 2;
	s_ = n_->SetWithExpireAt(key, val, timestamp);
	CHECK_STATUS(OK);
	if(s_.ok())
		flag1 = true;
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(OK);
	if(s_.ok())
		flag2 = true;
	EXPECT_EQ(val, getVal);
	sleep(3);
	s_ = n_->Get(key, &getVal);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("timestamp>当前时间，是否按期过期");
	else
		log_fail("timestamp>当前时间，是否按期过期");

	s_.OK();//timestamp<当前时间
	flag1 = false; flag2 = false; flag3 = false;
	key = GetRandomKey_();
	val = GetRandomVal_();
	timestamp = time(NULL) - 1;
	s_ = n_->SetWithExpireAt(key, val, timestamp);	
	CHECK_STATUS(OK);
	if(s_.ok())
		flag1 = true;
	n_->Get(key, &getVal);
	EXPECT_EQ(string(""), getVal);
	if(string("")==getVal)
		flag2 = true;
	n_->TTL(key, &ttl);
	EXPECT_EQ(-2, ttl);
	if(ttl == -2)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("timestamp<当前时间, 数据是否不写入");
	else
		log_fail("timestamp<当前时间, 数据是否不写入");	

	s_.OK();
	flag1 = false; flag2 = false; flag3 = false;
	key = GetRandomKey_();
	val = GetRandomVal_();
	timestamp = 0;
	s_ = n_->SetWithExpireAt(key, val, timestamp);
	CHECK_STATUS(OK);
	if(s_.ok())
		flag1 = true;
	n_->Get(key, &getVal);
	EXPECT_EQ(val, getVal);
	if(val == getVal)
		flag2 = true;
	n_->TTL(key, &ttl);
	EXPECT_EQ(-1, ttl);
	if(ttl == -1)
		flag3 = true;
	if(flag1 && flag2 && flag3)
		log_success("timestamp<=0,数据有效期是否无限, timestamp=%lld, ttl=%lld", timestamp, ttl);
	else
		log_fail("timestamp<=0,数据有效期是否无限, timestamp=%lld, ttl=%lld", timestamp, ttl);
	log_message("============================KVTEST END===========================");
	log_message("============================KVTEST END===========================\n\n");
}
