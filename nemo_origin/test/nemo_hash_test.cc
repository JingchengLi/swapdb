#include <string>
#include <vector>
#include <sys/time.h>
#include <cstdlib>
#include <cstdlib>
#include <string>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

//#include "stdint.h"
#include "nemo_hash_test.h"
using namespace std;


TEST_F(NemoHashTest, TestHSet)
{
	log_message("============================HASHTEST START===========================");
	log_message("============================HASHTEST START===========================");
#define LoopPositiveProcess(TestMessage) s_ = n_->HSet(key, field, val);\
					CHECK_STATUS(OK);\
    				n_->HGet(key, field, &getVal);\
					EXPECT_EQ(val, getVal);\
					if(s_.ok() && val == getVal)\
						log_success(TestMessage);\
					else\
						log_fail(TestMessage)
	log_message("========TestHSet========");
	string key, field, val, getVal;
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();

	s_.OK();//测试key不存在
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("测试key不存在的情况");
	else
		log_fail("测试key不存在的情况");

	s_.OK();//测试key存在的情况
	field = GetRandomField_();
	val = GetRandomVal_();
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("测试key存在的情况");
	else
		log_fail("测试key存在的情况");

	s_.OK();//测试key存在的情况，field存在的情况
	val = GetRandomVal_();
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("测试key存在的情况");
	else
		log_fail("测试key存在的情况");
	
	s_.OK();//测试val取最大长度
	val = GetRandomBytes_(maxValLen_);
	LoopPositiveProcess("测试val取最大长度");

	s_.OK();//测试val取空值
	val = "";
	LoopPositiveProcess("测试val取空值");
	
	s_.OK();//测试field取最大长度
	val = GetRandomVal_();
	field = GetRandomBytes_(maxFieldLen_);
	LoopPositiveProcess("测试field取最大长度");

	s_.OK();//测试field为空
	val = GetRandomVal_();
	field = "";
	LoopPositiveProcess("测试field为空");
	
	s_.OK();//测试key最大长度
	field = GetRandomField_();
	key = GetRandomBytes_(maxKeyLen_);
	LoopPositiveProcess("测试key最大长度");

	s_.OK();//测试key为空
	key = "";
	field = GetRandomField_();
	val = GetRandomVal_();
	LoopPositiveProcess("测试field为空");
}

TEST_F(NemoHashTest, TestHGet)
{
	log_message("\n========TestHSet========");
	
	#define GetLoopOKProcess(TestMessage) s_ = n_->HGet(key, field, &getVal);\
									  CHECK_STATUS(OK);\
									  EXPECT_EQ(val, getVal);\
									  if(s_.ok() && val == getVal)\
										  log_success(TestMessage);\
									  else\
										  log_fail(TestMessage)
	#define GetLoopNotFoundProcess(TestMessage) getVal = "";\
									  s_ = n_->HGet(key, field, &getVal);\
									  CHECK_STATUS(NotFound);\
									  EXPECT_EQ(true, getVal.empty());\
									  if(s_.IsNotFound() && getVal.empty())\
										  log_success(TestMessage);\
									  else\
										  log_fail(TestMessage)


	string key, field, val, getVal;

	s_.OK();//原来的key不存在；field不/存在；原来的val非空
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HDel(key, field);
	GetLoopNotFoundProcess("原来的key不存在；field不/存在；原来的val非空");

	s_.OK();//原来的key存在；field存在；原来的val非空
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	GetLoopOKProcess("原来的key存在；field存在；原来的val非空");
	
	s_.OK();//原来的key存在；field不存在；原来的val非空
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	GetLoopNotFoundProcess("原来的key存在；field不存在；原来的val非空");

	s_.OK();//原来的key存在；field存在；原来的val为空
	val = "";
	n_->HSet(key, field, val);
	GetLoopOKProcess("原来的key存在；field存在；原来的val为空");

	s_.OK();//原来的key存在；field存在；原来的val取最大长度
	val = GetRandomBytes_(maxValLen_);
	n_->HSet(key, field, val);
	GetLoopOKProcess("原来的key存在；field存在；原来的val取最大长度");	
}

TEST_F(NemoHashTest, TestHDel)
{
	log_message("\n========TestHDel========");
	string key1, field1, val1, key, field, val;
	
	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	field = GetRandomField_();
	s_ = n_->HDel(key, field);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("原来的key不存在");
	else
		log_fail("原来的key不存在");
	
	s_.OK();//原来的key存在；field存在
	val = GetRandomVal_();
	n_->HSet(key, field, val);
    s_ = n_->HDel(key, field);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("原来的key存在；field存在");
	else
		log_fail("原来的key存在；field存在");

	s_.OK();//原来的key存在；field不存在
	s_ = n_->HDel(key, field);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("原来的key存在；field不存在");
	else
		log_fail("原来的key存在；field不存在");
}

TEST_F(NemoHashTest, TestHExist)
{
	#define HExistLoopPositiveProcess(Message) isExist = n_->HExists(key, field);\
										EXPECT_EQ(true, isExist);\
										if(isExist)\
											log_success(Message);\
										else\
											log_fail(Message)
	#define HExistLoopNegativeProcess(Message) isExist = n_->HExists(key, field);\
										EXPECT_EQ(false, isExist);\
										if(!isExist)\
											log_success(Message);\
										else\
											log_fail(Message)
	log_message("\n========TestHExist========");
	string key, field, val;
	bool isExist;
	
	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	field = GetRandomField_();
	n_->HDel(key, field);
	HExistLoopNegativeProcess("原来的key不存在");

	s_.OK();//原来的key存在；field存在；val为空
	val = "";
	n_->HSet(key, field, val);
	HExistLoopPositiveProcess("原来的key存在；field存在；val为空");

	s_.OK();//原来的key存在；field存在；val非空
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	HExistLoopPositiveProcess("原来的key存在；field存在；val为空");
	
	s_.OK();//
	n_->HDel(key, field);
	HExistLoopNegativeProcess("原来的key存在；field不存在");
}

TEST_F(NemoHashTest, TestHKeys)
{
	log_message("\n========TestHKeys========");
	string key, field, val;
	vector<string> fields;

	s_.OK();//key不存在
	key = GetRandomKey_();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key不存在");
	else
		log_fail("key不存在");

	s_.OK();//key存在；没有field；
	fields.clear();
	field = GetRandomKey_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key存在；没有field");
	else
		log_fail("key存在；没有field");

	s_.OK();//key存在；有field；
	key = "HKey_Test";
	int fieldsNum = 100, fieldsIndexStart = 0;
	log_message("======设置key为%s, field从%s%d到%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart + fieldsNum; fieldsIndex++)
	{
		n_->HSet(key, key+"_"+itoa(fieldsIndex), itoa(fieldsIndex));
	}
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), *(fields.begin()));
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), *(fields.rbegin()));
	EXPECT_EQ(fieldsNum, fields.size());
	if(s_.ok() && *(fields.begin()) == key+"_"+itoa(fieldsIndexStart) && key+"_"+itoa(fieldsIndexStart+fieldsNum-1) == *(fields.rbegin()) && fieldsNum == fields.size())
		log_success("key存在；有field");
	else
		log_fail("key存在；有field");

	s_.OK();//key存在，有fields；但是key之后有别的结构的keyxx
	val = GetRandomVal_();
	n_->Set(key+"1", val);
	val = GetRandomVal_();
	n_->Set(key+"2", val);
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), *(fields.begin()));
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), *(fields.rbegin()));
	EXPECT_EQ(fieldsNum, fields.size());
	if(s_.ok() && *(fields.begin()) == key+"_"+itoa(fieldsIndexStart) && key+"_"+itoa(fieldsIndexStart+fieldsNum-1) == *(fields.rbegin()) && fieldsNum == fields.size())
		log_success("key存在，有fields；但是key之后有别的结构的keyxx");
	else
		log_fail("key存在，有fields；但是key之后有别的结构的keyxx");

	s_.OK();//key存在，但是不是hash结构
	key = GetRandomKey_();
	val = GetRandomKey_();
	n_->Set(key, val);
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key存在，但是不是hash结构");
	else
		log_fail("key存在，但是不是hash结构");

	fields.clear();
}

TEST_F(NemoHashTest, TestHGetall)
{
	log_message("\n========TestHGetall========");
	string key, field, val;
	vector<nemo::FV> fvs;
	
	s_.OK();//key存在；有fields；val值正常
	int fieldsNum = 100, fieldsIndexStart = 0;
	key = "HGetall_test";
	log_message("======设置key为%s, field从%s%d到%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart+fieldsNum; fieldsIndex++)
	{
		field = key + "_" + itoa(fieldsIndex);
		n_->HSet(key, field, itoa(fieldsIndex));
	}
	s_ = n_->HGetall(key, fvs);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), fvs.begin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart), fvs.begin()->val);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->val);
	EXPECT_EQ(fieldsNum, fvs.size());
	if(s_.ok() && key+"_"+itoa(fieldsIndexStart)==fvs.begin()->field && itoa(fieldsIndexStart)==fvs.begin()->val && key+"_"+itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->field && itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->val && fieldsNum==fvs.size())
		log_success("key存在；有fields；val值正常");
	else
		log_fail("key存在；有fields；val值正常");

	s_.OK();//key存在，有fieled；但是之后有别的KV的key*
	val = GetRandomVal_();
	n_->Set(key+"1", val);
	val = GetRandomVal_();
	n_->Set(key+"2", val);
	fvs.clear();
	s_ = n_->HGetall(key, fvs);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), fvs.begin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart), fvs.begin()->val);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->val);
	EXPECT_EQ(fieldsNum, fvs.size());
	if(s_.ok() && key+"_"+itoa(fieldsIndexStart)==fvs.begin()->field && itoa(fieldsIndexStart)==fvs.begin()->val && key+"_"+itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->field && itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->val && fieldsNum==fvs.size())
		log_success("key存在；有fields；val值正常");
	else
		log_fail("key存在；有fields；val值正常");
	fvs.clear();
}

TEST_F(NemoHashTest, TestHLen)
{
	log_message("\n========TestHLen========");
	string key, field, val;
	int64_t retLen;

	s_.OK();//key不存在/key和field对应的值不存在
	key = GetRandomKey_();
	field = GetRandomField_();
	retLen = n_->HLen(key);
	EXPECT_EQ(0, retLen);
	if(retLen == 0)
		log_success("key不存在/key和field对应的值不存在");
	else
		log_fail("key不存在/key和field对应的值不存在");

	s_.OK();//key存在；有fields；
	key = "HLen_Test";
	vector<string> fields;
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key, *iter);
	}
	int fieldsNum = GetRandomUint_(0, 256);
	int fieldsIndexStart = 0;
	log_message("======设置key为%s, field从%s%d到%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart+fieldsNum; fieldsIndex++)
	{
		field = key + "_" + itoa(fieldsIndex);
		n_->HSet(key, field, itoa(fieldsIndex));
	}
	retLen = n_->HLen(key);
	EXPECT_EQ(fieldsNum, retLen);
	if(retLen == fieldsNum)
		log_success("key存在；有fields");
	else
		log_fail("key存在；有fields");
}

TEST_F(NemoHashTest, TestHMSet)
{
	log_message("\n========TestHMSet========");
	string key, field, val, getVal;
	vector<nemo::FV> fvs;
	bool flag;
	int fvsNum = GetRandomUint_(1, maxHMSetNum_);
	for(int index = 0; index != fvsNum; index++)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		fvs.push_back({field, val});
	}
	
	s_.OK();//Key为空
	key = "";
	s_ = n_->HMSet(key, fvs);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("Key为空");
	else
		log_fail("Key为空");

	s_.OK();//Key非空；FS有值（正常数目）
	key = GetRandomKey_();
	s_ = n_->HMSet(key, fvs);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FV>::iterator iter = fvs.begin(); iter != fvs.end(); iter++)
	{
		n_->HGet(key, iter->field, &getVal);
		EXPECT_EQ(iter->val, getVal);
		if(getVal != iter->val)
		{
			flag = false;
			break;
		}
	}
	if(s_.ok() && flag)
		log_success("Key非空；FS有值（正常数目）");
	else
		log_fail("Key非空；FS有值（正常数目）");

	s_.OK();//Key非空：FS有值（最大数目）
	fvs.clear();
	fvsNum = maxHMSetNum_;
	for(int index = 0; index != fvsNum; index++)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		fvs.push_back({field, val});
	}
	s_ = n_->HMSet(key, fvs);
	flag = true;
	for(vector<nemo::FV>::iterator iter = fvs.begin(); iter != fvs.end(); iter++)
	{
		n_->HGet(key, iter->field, &getVal);
		EXPECT_EQ(iter->val, getVal);
		if(iter->val != getVal)
		{
			flag = false;
			break;
		}
	}
	if(s_.ok() && flag)
		log_success("Key非空：FS有值（最大数目）");
	else
		log_fail("Key非空：FS有值（最大数目）");
}

TEST_F(NemoHashTest, TestHMGet)
{
	log_message("\n========TestHMGet========");
	string key, field, val;
	vector<string> fields;
	vector<nemo::FV> fvs;
	vector<nemo::FVS>fvss;
	bool flag;

	s_.OK();//Key非空；fields有值(都有对应的val)（正常数目）
	key = "HMGet_Test";
	int32_t hgetNum = GetRandomUint_(1, maxHMGetNum_);
	while(hgetNum>0)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		n_->HSet(key, field, val);
		fields.push_back(field);
		fvs.push_back({field, val});
		hgetNum--;
	}
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(!((iter->status).ok()))
		{
			flag = false;
			break;
		}
	}
	EXPECT_EQ(true, flag);
	if(flag == true && s_.ok())
		log_success("Key非空；fields有值(都有对应的val)（正常数目）");
	else
		log_fail("Key非空；fields有值(都有对应的val)（正常数目）");

	s_.OK();//Key非空；fields有值(有没有对应的val)（正常数目）
	int32_t arbiterNum = GetRandomUint_(0, hgetNum);
	int32_t index = 0;
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		if(index == arbiterNum)
		{
			n_->HDel(key, *iter);
			break;
		}
		index++;
	}
	fvss.clear();
	fvs.clear();
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	index = 0;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(index != arbiterNum && !((iter->status).ok()))
		{
			flag = false;
			break;
		}
		if(index == arbiterNum && (iter->status).ok())
		{
			flag =  false;
			break;
		}
		index++;
	}
	EXPECT_EQ(true, flag);
	if(s_.ok() && true == flag)
		log_success("Key非空；fields有值(有没有对应的val)（正常数目）");
	else
		log_fail("Key非空；fields有值(有没有对应的val)（正常数目）");

	s_.OK();//Key非空；fields有值(都有对应的val)（最大数目）
	fields.clear();
	fvs.clear();
	fvss.clear();
	hgetNum = maxHMGetNum_;
	while(hgetNum>0)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		n_->HSet(key, field, val);
		fields.push_back(field);
		fvs.push_back({field, val});
		hgetNum--;
	}
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(!((iter->status).ok()))
		{
			flag = false;
			break;
		}
	}
	EXPECT_EQ(true, flag);
	if(flag == true && s_.ok())
		log_success("Key非空；fields有值(都有对应的val)（最大数目）");
	else
		log_fail("Key非空；fields有值(都有对应的val)（最大数目）");	
	fields.clear();
	fvs.clear();
	fvss.clear();
}

TEST_F(NemoHashTest, TestHSetnx)
{
	log_message("\n========TestHSetnx========");

	string key, field, val;
	
	s_.OK();//key，fields对应的原来值是不存在的
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	s_ = n_->HSetnx(key, field, val);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("key，fields对应的原来值是不存在的");
	else
		log_fail("key，fields对应的原来值是不存在的");

	s_.OK();//key，fields对应的原来值是存在的
	val = GetRandomVal_();
	s_ = n_->HSetnx(key, field, val);
	CHECK_STATUS(Corruption);
	if(s_.IsCorruption())
		log_success("key，fields对应的原来值是存在的");
	else
		log_fail("key，fields对应的原来值是存在的");
}

TEST_F(NemoHashTest, TestHStrlen)
{
	log_message("\n========TestHStrlen========");
	string key, field, val;
	int64_t retValLen;

	s_.OK();//key和field对应的值不存在
	key = GetRandomKey_();
	field = GetRandomField_();
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(0, retValLen);
	if(0 == retValLen)
		log_success("key和field对应的值不存在");
	else
		log_fail("key和field对应的值不存在");

	s_.OK();//key和field对应的值；val长度为0
	val = "";
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(0, retValLen);
	if(0 == retValLen)
		log_success("key和field对应的值；val长度为0");
	else
		log_fail("key和field对应的值；val长度为0");

	s_.OK();//key和field对应的值；val长度一般
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(val.length(), retValLen);
	if(val.length() == retValLen)
		log_success("key和field对应的值；val长度一般");
	else
		log_fail("key和field对应的值；val长度一般");

	s_.OK();//key和field对应的值；val长度最大
	val = GetRandomBytes_(maxValLen_);
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	//EXPECT_EQ(maxValLen_, retValLen);	
	EXPECT_EQ(val.length(), retValLen);
	if(maxValLen_ == retValLen)
		log_success("key和field对应的值；val长度最大");
	else
		log_fail("key和field对应的值；val长度最大");
}


TEST_F(NemoHashTest, TestHScan)
{
	
	log_message("\n========TestHScan========");
	string key, field, val, start, end;
	vector<string>fields;
	nemo::HIterator *hiter;
	key = "HScan_Test";
	int32_t count, startInt, endInt, totalFieldsNum = 100, numPre = 10000, index;
	uint64_t limit = -1;
	bool flag;
	
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
    fprintf (stderr, "hdel (%s)\n", iter->c_str());
		n_->HDel(key, *iter);
	}

	s_.OK();//key非空；start非空；end非空；但是key上不是hash结构
	val = GetRandomVal_();
	n_->Set(key, val);
	start = "";
	end = "";
	hiter = n_->HScan(key, start, end, limit);
	count = 0;
	while(hiter->Valid())
	{
		count++;
		hiter->Next();
	}
	EXPECT_EQ(0, count);
	if(0 == count)
		log_success("key非空；start非空；end非空；但是key上不是hash结构");
	else
		log_fail("key非空；start非空；end非空；但是key上不是hash结构");

    int64_t del_ret;
	n_->Del(key, &del_ret);
	delete hiter;
	
	log_message("========Key is %s, field from %s_%d to %s_%d", key.c_str(), key.c_str(), numPre+0, key.c_str(), numPre+totalFieldsNum-1);
	for(int32_t index = 0; index != totalFieldsNum; index++)
	{
		n_->HSet(key, key + "_" + itoa(numPre+index), itoa(numPre+index));
	}
	
#define HScanLoopProcess(endCompare, limit, message)  \
	hiter = n_->HScan(key, start, end, limit);\
	index = startInt;\
	flag = true;\
  for (; hiter->Valid(); hiter->Next())\
	{\
		EXPECT_EQ(key + "_" + itoa(numPre + index), hiter->field());\
		if(key + "_" + itoa(numPre + index) != hiter->field())\
		{\
			flag = false;\
		}\
		index++;\
	}\
	EXPECT_EQ(endCompare, index);\
	if(flag && (endCompare == index))\
		log_success(message);\
	else\
		log_fail(message);\
	delete hiter

	s_.OK();//key非空；start非空；end非空；但是key和start、end对应的数据不存在
	startInt = totalFieldsNum + 10;
	endInt = totalFieldsNum + 20;
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt, limit, "key非空；start非空；end非空；但是key和start、end对应的数据不存在");




	s_.OK();//key非空；start和end非空，且都在keys范围内（正常情况）
	startInt = GetRandomUint_(0, totalFieldsNum);
	endInt = GetRandomUint_(startInt, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(endInt+1, limit, "key非空；start和end非空，且都在keys范围内（正常情况）");


	
	s_.OK();//测试limit为正数，是否起作用了
	startInt = GetRandomUint_(0, totalFieldsNum-10);
	endInt = GetRandomUint_(startInt+5, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt+3, 3, "测试limit为正数，是否起作用了");
	

	s_.OK();//key非空；start和end非空，且都在keys范围内（start>end）
	startInt = GetRandomUint_(1, totalFieldsNum);
	endInt = GetRandomUint_(0, startInt-1);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt, limit, "key非空；start和end非空，且都在keys范围内（start>end）");

	s_.OK();//key非空；start非空，end为空
	startInt = GetRandomUint_(0, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = "";
	HScanLoopProcess(totalFieldsNum, limit, "key非空；start非空，end为空");

	s_.OK();//start为空；end不为空
	startInt = 0;
	endInt = GetRandomUint_(0, totalFieldsNum);
	start = "";
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(endInt+1, limit, "start为空；end不为空");

	s_.OK();//key非空；start为空；end为空
	startInt = 0;
	start = "";
	end = "";
	HScanLoopProcess(totalFieldsNum, limit, "key非空；start为空；end为空");

	string key2, field2, val2;
	int32_t totalFieldsNum2 = 50;
	key2 = "";
	
	s_.OK();//key空；start为空；end为空
	fields.clear();
	n_->HKeys(key2, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++) {
		n_->HDel(key2, *iter);
	}
	for(int32_t index2 = 0; index2 != totalFieldsNum2; index2++)
	{
		field2 = key2 + "_" + itoa(numPre+index2);
		n_->HSet(key2, field2, itoa(numPre+index2));
	}
	hiter = n_->HScan("", "", "", -1);
	index = 0;
  for (; hiter->Valid(); hiter->Next())
	{
		index++;
	}
	EXPECT_EQ(totalFieldsNum2, index);
	if(totalFieldsNum2 == index)
		log_success("key空；start为空；end为空");
	else
		log_fail("key空；start为空；end为空");
	delete hiter;
	
}

TEST_F(NemoHashTest, TestHVals)
{
	log_message("\n========TestHVals========");
	string key, field, val;
	vector<string> vals;
	vector<string> fields;
	bool flag;

	s_.OK();//key存在；但是key不是hash结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, vals.empty());
	if(s_.ok() && vals.empty())
		log_success("key存在；但是key不是hash结构");
	else
		log_success("key存在；但是key不是hash结构");
    int64_t del_ret;
	n_->Del(key, &del_ret);

	s_.OK();//key存在；没有fields
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	val.clear();
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, vals.empty());
	if(s_.ok() && vals.empty())
		log_success("key存在；没有fields");
	else
		log_fail("key存在；没有fields");

	
	int32_t totalFieldsNum = 100;
	int32_t numPre = 10000;
	key = "HVals_test";
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key, *iter);
	}
	for(int32_t index = 0; index != totalFieldsNum; index++)
	{
		field = key + "_" +	itoa(numPre+index);
		val = itoa(numPre+index);
		n_->HSet(key, field, val);
	}

	s_.OK();//key存在；有fields；
	vals.clear();
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(totalFieldsNum, vals.size());
	if(totalFieldsNum == vals.size() && s_.ok())
		log_success("key存在；有fields");
	else
		 log_fail("key存在；有fields");

	s_.OK();//key为空；
	string key2 = "", field2, val2;
	int32_t totalFieldsNum2 = 300;
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key2, *iter);
	}
	for(int32_t index = 0; index != totalFieldsNum2; index++)
	{
		field2 = key2 + "_" + itoa(numPre+index);
		val2 = itoa(numPre+index);
		n_->HSet(key2, field2, val2);
	}
	vals.clear();
	s_ = n_->HVals("", vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(totalFieldsNum2,vals.size());
	if(s_.ok() &&  vals.size() == totalFieldsNum2)
		log_success("key为空");
	else
		log_fail("key为空");

	vals.clear();
	fields.clear();
}


TEST_F(NemoHashTest, TestHIncrby)
{
	log_message("\n========TestHIncrby========");
	string key, field, val, newVal;
	int64_t by;

#define HIncybyPositiveLoopProcess(result, message) \
	s_ = n_->HIncrby(key, field, by, newVal);\
	CHECK_STATUS(OK);\
	EXPECT_EQ(result, atoi(newVal.c_str()));\
	if(s_.ok() && result == atoi(newVal.c_str()))\
		log_success(message);\
	else\
		log_fail(message)

#define HIncybyNegativeLoopProcess(status, result, message) \
	s_ = n_->HIncrby(key, field, by, newVal);\
	CHECK_STATUS(status);\
	EXPECT_EQ(result, atoi(newVal.c_str()));\
	if(!(s_.ok()) && result == atoi(newVal.c_str()))\
		log_success(message);\
	else\
		log_fail(message)

	s_.OK();//key和field对应的val是正常数值；incrby正常数值
	key = GetRandomKey_();
	field = GetRandomField_();
	val = "13";
	n_->HSet(key, field, val);
	by = 4;
	s_ = n_->HIncrby(key, field, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(17, atoi(newVal.c_str()));
	if(s_.ok() && 17 == atoi(newVal.c_str()))
		log_success("key和field对应的val是正常数值；incrby正常数值");
	else
		log_fail("key和field对应的val是正常数值；incrby正常数值");

	by = 4;
	s_.OK();//key和field对应的值不存在
	n_->HSet(key, field, "2");
	n_->HDel(key, field);
	newVal = "0";
	HIncybyPositiveLoopProcess(by, "key和field对应的值不存在");

	s_.OK();//key和field对应的val只有一个加号
	val = "+";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key和field对应的val只有一个加号");

	s_.OK();//key和field对应的val只有一个减号
	val = "-";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key和field对应的val只有一个减号");
	
	s_.OK();//key和field对应的val不全是数字
	val = "123#A";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key和field对应的val不全是数字");

	s_.OK();//key和field对应val为“0000”
	val = "00000";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(5, "key和field对应val为“0000”");

	s_.OK();//key和field对应的val含有+号，如：“+10”
	val = "+10";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(15, "key和field对应的val含有+号，如：“+10”");

	s_.OK();//key和field对应的val含有+号，如：“-10”
	val = "-10";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(-5, "key和field对应的val含有+号，如：“-10");

	s_.OK();//结果过大/小，导致溢出
	val = to_string(LLONG_MAX);
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyNegativeLoopProcess(Invalid, 0, "结果过大/小，导致溢出");
}

TEST_F(NemoHashTest, TestHIncrbyfloat)
{
	log_message("\n========TestHIncrbyfloat========");
	string key, field, val, newVal;
	double by, diff;

	s_.OK();//key和field对应的是正常的数值；floatby是正常数值
	key = GetRandomKey_();
	field = GetRandomField_();
	val = "12.3";
	by = 1.2;
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key和field对应的是正常的数值；floatby是正常数值");
	else
		log_fail("key和field对应的是正常的数值；floatby是正常数值");

	s_.OK();//key和field对应的值不存在
	n_->HDel(key, field);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key和field对应的值不存在");
	else
		log_fail("key和field对应的值不存在");

	s_.OK();//key和field对应的原val有非数字字符
	val = "1.23jfkdj";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key和field对应的原val有非数字字符");
	else
		log_fail("key和field对应的原val有非数字字符");

	s_.OK();//key和field对应的值只有一个+
	val = "+";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key和field对应的值只有一个+");
	else
		log_fail("key和field对应的值只有一个+");

	s_.OK();//key和field对应的值只有一个-
	val = "-";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key和field对应的值只有一个-");
	else
		log_fail("key和field对应的值只有一个-");

	s_.OK();//key和field对应的值以“+”开头
	val = "+11.3";
	by = 23.4;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key和field对应的值以+开头");
	else
		log_fail("key和field对应的值以+开头");

	s_.OK();//key和field对应的值以“+”开头
	val = "-13.4";
	by = 22.34;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key和field对应的值以-开头");
	else
		log_fail("key和field对应的值以-开头");

	s_.OK();//所加的结果的小数位数是0
	val = "3.34";
	by = 7.66;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(string("11"), newVal);
	if(s_.ok() && string("11") == newVal)
		log_success("所加的结果的小数位数是0");
	else
		log_fail("所加的结果的小数位数是0");

	log_message("============================HASHTEST END===========================");
	log_message("============================HASHTEST END===========================\n\n");
}
