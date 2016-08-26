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
#include "nemo_list_test.h"
using namespace std;

TEST_F(NemoListTest, TestLPush)
{
	log_message("============================LISTTEST START===========================");
	log_message("============================LISTTEST START===========================");
	log_message("========TestLPush========");
	string key, val;
	int64_t llen;
	
	s_.OK();//原来的key存在，存的不是list结构数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if (s_.ok() && 1 == llen) {
		log_success("原来的key存在，存的不是list结构数据");
	} else {
		log_fail("原来的key存在，存的不是list结构数据");
	}
	n_->LPop(key, &val);

	s_.OK();//原来的key不存在；
	key = GetRandomKey_();
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if (s_.ok() && 1 == llen) {
		log_success("原来的key不存在");
	} else {
		log_fail("原来的key不存在");
	}

	s_.OK();//原来的key存在，存的是list结构数据
	val = GetRandomVal_();
	llen = 1;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(2, llen);
	if (s_.ok() && 2 == llen) {
		log_success("原来的key存在，存的是list结构数据");
	} else {
		log_fail("原来的key存在，存的是list结构数据");
	}

	s_.OK();//val取最大长度
	val = GetRandomBytes_(maxValLen_);
	llen = 2;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(3, llen);
	if (s_.ok() && 3 == llen) {
		log_success("al取最大长度");
	} else {
		log_fail("al取最大长度");
	}

	s_.OK();//val取空值
	val = "";
	llen = 3;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(4, llen);
	if (s_.ok() && 4 == llen) {
		log_success("val取空值");
	} else {
		log_fail("val取空值");
	}
}

TEST_F(NemoListTest, TestLIndex)
{
	log_message("\n========TestLIndex========");
	string key, val, valResult;
	int64_t index;
	
#define LIndexLoopProcess(index, state, valResult, message)\
	s_ = n_->LIndex(key, index, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if (string(#state) == "OK") {\
		if (s_.ok() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)


	s_.OK();//key不存在
	key = GetRandomKey_();
	index = 0;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, NotFound, valResult, "key不存在");

	s_.OK();//key存在，存的不是list结构数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	index = 0;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, NotFound, valResult, "key存在，存的不是list结构数据");

	key = "LIndex_Test";
	int64_t len = 50, tempLen;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LIndex_Test_") + itoa(idx), &tempLen);
	}
	log_message("===List from left to right is LIndex_Test_0 to LIndex_Test_49");
	
	s_.OK();//key存在，存的是list 数据，index为正数在list范围内
	index = GetRandomUint_(0, len-1);
	LIndexLoopProcess(index, OK, string("LIndex_Test_") + itoa(index), "key存在，存的是list 数据，index为正数在list范围内");

	s_.OK();//key存在，存的是list 数据，index为正数，超出list范围
	index = len;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, Corruption, valResult, "key存在，存的是list 数据，index为正数，超出list范围");

	s_.OK();//key存在，存的是list 数据，index为负数，在list范围
	index = GetRandomUint_(1, len);
	index = (-1)*index;
	LIndexLoopProcess(index, OK, string("LIndex_Test_") + itoa(len+index), "key存在，存的是list 数据，index为负数，在list范围");

	s_.OK();//key存在，存的是list 数据，index为负数，超出list范围
	index = (len+10)*(-1);
	valResult = "";
	val = "";
	LIndexLoopProcess(index, Corruption, valResult, "key存在，存的是list 数据，index为负数，超出list范围");
}

TEST_F(NemoListTest, TestLLen)
{
	log_message("\n========TestLLen========");
	string key, val;
	int64_t llen;

	s_.OK();//key不存在
	key = GetRandomKey_();
	s_ = n_->LLen(key, &llen);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, llen);
	if (s_.IsNotFound() && 0 == llen) {
		log_success("key不存在");
	} else {
		log_fail("key不存在");
	}

	s_.OK();//key存在，但存的不是list结构数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	s_ = n_->LLen(key, &llen);
	EXPECT_EQ(0, llen);
	if (s_.IsNotFound() && 0 == llen) {
		log_success("key不存在");
	} else {
		log_fail("key不存在");
	}

	
	s_.OK();//Key存在，且存有list数据结构
	key = "LLen_Test";
	int64_t len = 50, tempLen;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LIndex_Test_") + itoa(idx), &tempLen);
	}
	s_ = n_->LLen(key, &llen);
	CHECK_STATUS(OK);	
	EXPECT_EQ(len, llen);
	if (s_.ok() && llen == len) {
		log_success("Key存在，且存有list数据结构");
	} else {
		log_fail("Key存在，且存有list数据结构");
	}
}

TEST_F(NemoListTest, TestLPop)
{
	log_message("\n========TestLPop========");
#define LPopLoopProcess(state, valResult, message)\
	s_ = n_->LPop(key, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if (string(#state) == "OK") {\
		if (s_.ok() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)

	string key, val, valResult;
	int64_t len, tempLen;
	bool flag;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	val = "";
	valResult = "";
	LPopLoopProcess(NotFound, valResult, "原来的key不存在");

	s_.OK();//原来的key存的不是list结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = "";
	valResult = "";
	LPopLoopProcess(NotFound, valResult, "原来的key存的不是list结构");

	s_.OK();//原来的key内有list的数据结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &len);
	valResult = val;
	LPopLoopProcess(OK, valResult, "原来的key内有list的数据结构");

	s_.OK();//连续多次LPush数据，观察LPop是否逆序吐出
	key = "LPop_Test";
	len = 50;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LPop_Test_") + itoa(idx), &tempLen);
	}
	flag = true;
	for(int64_t idx = 0; idx != len; idx++)
	{
		s_ = n_->LPop(key, &val);
		CHECK_STATUS(OK);
		EXPECT_EQ(string("LPop_Test_")+itoa(idx), val);
		if(!s_.ok() || val != string("LPop_Test_")+itoa(idx))
			flag = false;
	}
	if (flag) {
		log_success("连续多次Push数据，观察LPop是否逆序吐出");
	} else {
		log_fail("连续多次Push数据，观察LPop是否逆序吐出");
	}
}

TEST_F(NemoListTest, TestLPushx)
{
#define LPushxLoopProcess(state, llenResult, message)\
	s_ = n_->LPushx(key, val, &llen);\
	CHECK_STATUS(state);\
	EXPECT_EQ(llenResult, llen);\
	if (string(#state) == "OK") {\
		if (s_.ok() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)

	log_message("\n========TestLPushx========");
	string key, val;
	int64_t llen, tempLlen;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	val = GetRandomVal_();
	LPushxLoopProcess(NotFound, 0, "原来的key不存在");

	s_.OK();//key存在，存的是数据结构，但是没有list数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	LPushxLoopProcess(NotFound, 0, "key存在，存的是数据结构，但是没有list数据");

	s_.OK();//key存在，且原来存的是list数据结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &llen);
	val = GetRandomVal_();
	tempLlen = llen+1;
	LPushxLoopProcess(OK, tempLlen, "key存在，且原来存的是list数据结构");
}

TEST_F(NemoListTest, TestLRange)
{
	log_message("\n========TestLRange========");

	string key, val, tempVal;
	int64_t llen, tempLLen, begin, end;
	vector<nemo::IV> ivs;

	s_.OK();//Key不存在
	key = GetRandomKey_();
	n_->LLen(key, &tempLLen);
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("Key不存在");
	else
		log_fail("Key不存在");

	s_.OK();//key存在，存的不是list结构数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key,val);
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("Key不存在");
	else
		log_fail("Key不存在");

	s_.OK();//key存在，存的是数据结构，但是没有list数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("key存在，存的是数据结构，但是没有list数据");
	else
		log_fail("key存在，存的是数据结构，但是没有list数据");


	llen = 50;
	key = "LRange_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);	
		tempLLen--;
	}
	for(int64_t idx = 0; idx != llen; idx++)
	{
		n_->RPush(key, string("LRange_Test_") + itoa(idx), &tempLLen);
	}
	log_message("===List from left to right is LRange_Test_0 to LRange_Test_%lld", llen);

	string startVal, endVal;

#define LRangeLoopProcess(ivsNum, startVal, EndVal, message)\
	ivs.clear();\
	s_ = n_->LRange(key, begin, end, ivs);\
	CHECK_STATUS(OK);\
	if (ivsNum > 0) {\
		EXPECT_EQ(ivsNum, ivs.size());\
		EXPECT_EQ(startVal, ivs.front().val);\
		EXPECT_EQ(endVal, ivs.back().val);\
		if(s_.ok() && ((ivsNum) == ivs.size()) && ((startVal) == ivs.front().val) && ((endVal) == ivs.back().val))\
			log_success(message);\
		else\
			log_fail(message);\
	} else {\
		EXPECT_EQ(0, ivs.size());\
		if(s_.ok() && 0 == ivs.size())\
			log_success(message);\
		else\
			log_fail(message);\
	}\
	do { } while(false)


	


	s_.OK();//key存在，有list结构数据；0<begin<end<length在有效范围内
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end-begin+1, startVal, endVal, "key存在，有list结构数据；0<begin<end<length在有效范围内");
	
	s_.OK();//key存在，有list结构数据；-length<begin<0, end在有效范围内end
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	begin = begin-llen;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end-begin-llen+1, startVal, endVal, "key存在，有list结构数据；-length<begin<0, end在有效范围内end");

	s_.OK();//key存在，有list结构数据；begin <-length,end 在有效范围内end
	begin = (-1)*(llen+10);
	end = GetRandomUint_(0, llen-1);
	n_->LIndex(key, 0, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end+1, startVal, endVal, "key存在，有list结构数据；-length<begin<0, end在有效范围内end");

	s_.OK();//key存在，有list结构数据；begin > length,end 在有效范围内end
	begin = llen+10;
	end = GetRandomUint_(0, llen-1);
	startVal = "";
	endVal = "";
	LRangeLoopProcess(0, startVal, endVal, "key存在，有list结构数据；begin > length,end 在有效范围内end");

	s_.OK();//key存在，有list结构数据；begin在有效范围内,-length<end<0
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	end = end-llen;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(llen+end-begin+1, startVal, endVal, "key存在，有list结构数据；begin在有效范围内,-length<end<0");

	s_.OK();//key存在，有list结构数据；begin在有效范围内,end<-length
	begin = GetRandomUint_(0, llen-1);
	end = (-1)*(llen+20);
	startVal = "";
	endVal = "";
	LRangeLoopProcess(0, startVal, endVal, "key存在，有list结构数据；begin在有效范围内,end<-length");

	s_.OK();//key存在，有list结构数据；begin在有效范围内,end>length
	begin = GetRandomUint_(0, llen-1);
	end = llen+20;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, llen-1, &endVal);
	LRangeLoopProcess(llen-begin, startVal, endVal, "key存在，有list结构数据；begin在有效范围内,end>length");

	ivs.clear();
}

TEST_F(NemoListTest, TestLSet)
{
	log_message("\n========TestLSet========");
	string key, val, getVal;

	s_.OK();//Key不存在
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->LSet(key, 0, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key不存在");
	else
		log_fail("Key不存在");

	s_.OK();//Key存在，存的是不是List结构
	n_->Set(key, val);
	val = GetRandomVal_();
	s_ = n_->LSet(key, 0, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key存在，存的是不是List结构");
	else
		log_fail("Key存在，存的是不是List结构");


	int64_t llen = 10, tempLLen, index;
	key = "LSet_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &getVal);	
		tempLLen--;
	}
	for(int64_t idx = 0; idx != llen; idx++)
	{
		n_->RPush(key, string("LRange_Test_") + itoa(idx), &tempLLen);
	}
	log_message("===List from left to right is LSet_Test_0 to LSet_Test_%lld", llen);

	s_.OK();//Key存在，存的是List结构，Index在list范围内；index为正
	val = GetRandomVal_();
	index = (int64_t)(GetRandomUint_(0, llen-1));
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(OK);
	n_->LIndex(key, index, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("Key存在，存的是List结构，Index在list范围内；index为正");
	else
		log_fail("Key存在，存的是List结构，Index在list范围内；index为正");

	s_.OK();//Key存在，存的是List结构，Index在list范围内；index为负
	val = GetRandomVal_();
	index = (-1)*(int64_t)(GetRandomUint_(1, llen));
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(OK);
	n_->LIndex(key, index, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("Key存在，存的是List结构，Index在list范围内；index为负");
	else
		log_fail("Key存在，存的是List结构，Index在list范围内；index为负");

	s_.OK();//Key存在，存的是List结构，Index不在list范围内
	val = GetRandomVal_();
	index = llen+10;
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(Corruption);
	if(s_.IsCorruption())
		log_success("Key存在，存的是List结构，Index不在list范围内");
	else
		log_fail("Key存在，存的是List结构，Index不在list范围内");
}

TEST_F(NemoListTest, TestLTrim)
{
	log_message("\n========TestLTrim========");
	string key, val;
	int64_t begin, end, llen, llenResult;

	s_.OK();//Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空
	key = GetRandomKey_();
	s_ = n_->LTrim(key, 0, 0);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");
	else
		log_fail("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");
	
	llen = 10;
	key = "HTrim_Test";

	s_.OK();//key存在，有list结构数据；0<begin<end<length在有效范围内
	write_list(llen, key);
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end-begin+1, llenResult);
	if(s_.ok() && llenResult == (end-begin+1))
		log_success("key存在，有list结构数据；0<begin<end<length在有效范围内");
	else
		log_fail("key存在，有list结构数据；0<begin<end<length在有效范围内");

	s_.OK();//key存在，有list结构数据；-length<begin<0, end在有效范围内end
	write_list(llen, key);
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	begin = begin-llen;
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end-begin-llen+1, llenResult);
	if(s_.ok() && (end-begin-llen+1)==llenResult)
		log_success("key存在，有list结构数据；-length<begin<0, end在有效范围内end");
	else
		 log_fail("key存在，有list结构数据；-length<begin<0, end在有效范围内end");

	s_.OK();//key存在，有list结构数据；begin <-length,end 在有效范围内end
	write_list(llen, key);
	begin = (-1)*(llen+1);
	end = GetRandomUint_(0, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end+1, llenResult);
	if(s_.ok() && (end+1)==llenResult)
		log_success("key存在，有list结构数据；begin <-length,end 在有效范围内end");
	else
		log_fail("key存在，有list结构数据；begin <-length,end 在有效范围内end");

	s_.OK();//key存在，有list结构数据；begin > length,end 在有效范围内end
	write_list(llen, key);
	begin = llen;
	end = GetRandomUint_(0, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0 == llenResult)
		log_success("key存在，有list结构数据；begin > length,end 在有效范围内end");
	else
		log_fail("key存在，有list结构数据；begin > length,end 在有效范围内end");

	s_.ok();//key存在，有list结构数据；begin在有效范围内,-length<end<0
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	end = end-llen;
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end+llen-begin+1, llenResult);
	if(s_.ok() && (end+llen-begin+1)==llenResult)
		log_success("key存在，有list结构数据；begin在有效范围内,-length<end<0");
	else
		log_fail("key存在，有list结构数据；begin在有效范围内,-length<end<0");

	s_.OK();//key存在，有list结构数据；begin在有效范围内,end<-length
	begin = GetRandomUint_(0, llen-1);
	end = (-1)*(llen+1);
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);	
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0==llenResult)
		log_success("key存在，有list结构数据；begin在有效范围内,end<-length");
	else
		log_fail("key存在，有list结构数据；begin在有效范围内,end<-length");
	
	s_.OK();//key存在，有list结构数据；begin在有效范围内,end>length
	begin = GetRandomUint_(0, llen-1);
	end = llen;
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(llen-begin, llenResult);
	if(s_.ok() && (llen-begin)==llenResult)
		log_success("key存在，有list结构数据；begin在有效范围内,end<-length");
	else
		log_fail("key存在，有list结构数据；begin在有效范围内,end<-length");

	s_.ok();//key存在，有list结构数据；0<=end<begin<length
	begin = GetRandomUint_(1, llen-1);
	end = GetRandomUint_(0, begin-1);
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0 == llenResult)
		log_success("key存在，有list结构数据；0<=end<begin<length");
	else
		log_fail("key存在，有list结构数据；0<=end<begin<length");
}

TEST_F(NemoListTest, TestRPush)
{
	log_message("\n========TestRPush========");
	string key, val;
	int64_t llen;
	
	s_.OK();//原来的key存在，存的不是list结构数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if(s_.ok() && 1 == llen)
		log_success("原来的key存在，存的不是list结构数据");
	else
		log_fail("原来的key存在，存的不是list结构数据");
	n_->LPop(key, &val);

	s_.OK();//原来的key不存在；
	key = GetRandomKey_();
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if(s_.ok() && 1 == llen)
		log_success("原来的key不存在");
	else
		log_fail("原来的key不存在");

	s_.OK();//原来的key存在，存的是list结构数据
	val = GetRandomVal_();
	llen = 1;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(2, llen);
	if(s_.ok() && 2 == llen)
		log_success("原来的key存在，存的是list结构数据");
	else
		log_fail("原来的key存在，存的是list结构数据");

	s_.OK();//val取最大长度
	val = GetRandomBytes_(maxValLen_);
	llen = 2;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(3, llen);
	if(s_.ok() && 3 == llen)
		log_success("al取最大长度");
	else
		log_fail("al取最大长度");

	s_.OK();//val取空值
	val = "";
	llen = 3;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(4, llen);
	if(s_.ok() && 4 == llen)
		log_success("val取空值");
	else
		log_fail("val取空值");
}

TEST_F(NemoListTest, TestRPushx)
{
#define RPushxLoopProcess(state, llenResult, message)\
	s_ = n_->RPushx(key, val, &llen);\
	CHECK_STATUS(state);\
	EXPECT_EQ(llenResult, llen);\
	if(string(#state) == "OK")\
		if(s_.ok() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "Corruption")\
		if(s_.IsCorruption() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "NotFound")\
		if(s_.IsNotFound() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message)

	log_message("\n========TestRPushx========");
	string key, val;
	int64_t llen, tempLlen;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	val = GetRandomVal_();
	RPushxLoopProcess(NotFound, 0, "原来的key不存在");

	s_.OK();//key存在，存的是数据结构，但是没有list数据
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	RPushxLoopProcess(NotFound, 0, "key存在，存的是数据结构，但是没有list数据");

	s_.OK();//key存在，且原来存的是list数据结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &llen);
	val = GetRandomVal_();
	tempLlen = llen+1;
	RPushxLoopProcess(OK, tempLlen, "key存在，且原来存的是list数据结构");
}

TEST_F(NemoListTest, TestRPop)
{
	log_message("\n========TestRPop========");
#define RPopLoopProcess(state, valResult, message)\
	s_ = n_->RPop(key, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if(string(#state) == "OK")\
		if(s_.ok() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "Corruption")\
		if(s_.IsCorruption() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "NotFound")\
		if(s_.IsNotFound() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message)

	string key, val, valResult;
	int64_t len, tempLen;
	bool flag;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	val = "";
	valResult = "";
	RPopLoopProcess(NotFound, valResult, "原来的key不存在");

	s_.OK();//原来的key存的不是list结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = "";
	valResult = "";
	RPopLoopProcess(NotFound, valResult, "原来的key存的不是list结构");

	s_.OK();//原来的key内有list的数据结构
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->RPush(key, val, &len);
	valResult = val;
	RPopLoopProcess(OK, valResult, "原来的key内有list的数据结构");

	s_.OK();//连续多次RPush数据，观察RPop是否逆序吐出
	key = "RPop_Test";
	len = 50;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->RPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->LPush(key, string("RPop_Test_") + itoa(idx), &tempLen);
	}
	flag = true;
	for(int64_t idx = 0; idx != len; idx++)
	{
		s_ = n_->RPop(key, &val);
		CHECK_STATUS(OK);
		EXPECT_EQ(string("RPop_Test_")+itoa(idx), val);
		if(!s_.ok() || val != string("RPop_Test_")+itoa(idx))
			flag = false;
	}
	if(flag)
		log_success("连续多次Push数据，观察RPop是否逆序吐出");
	else
		log_fail("连续多次Push数据，观察RPop是否逆序吐出");
}

TEST_F(NemoListTest, TestRPopLPush)
{
	log_message("\n========TestRPopLPush========");
	string keySrc, keyDst, valSrc, valDst, val;
	int64_t llen;

	
	s_.OK();//srcKey不存在；
	keySrc = GetRandomKey_();
	keyDst = GetRandomKey_();
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("srcKey不存在");
	else
		log_fail("srcKey不存在");
	
	s_.OK();//srcKey存在，有list数据结构，dstKey不存在
	valSrc = GetRandomVal_();
	n_->LPush(keySrc, valSrc, &llen);
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(OK);
	n_->LIndex(keyDst, 0, &valDst);
	EXPECT_EQ(valSrc, valDst);
	if(s_.ok() && valSrc == valDst)
		log_success("srcKey存在，有list数据结构，dstKey不存在");
	else
		log_fail("srcKey存在，有list数据结构，dstKey不存在");

	s_.OK();//srcKey存在，有list数据结构，dstKey存在，且有list 数据结构
	valSrc = GetRandomVal_();
	n_->RPush(keySrc, valSrc, &llen);
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(OK);
	n_->LIndex(keyDst, 0, &valDst);
	EXPECT_EQ(valSrc, valDst);
	if(s_.ok() && valSrc==valDst)
		log_success("srcKey存在，有list数据结构，dstKey存在，且有list 数据结构");
	else
		log_fail("srcKey存在，有list数据结构，dstKey存在，且有list 数据结构");
}

TEST_F(NemoListTest, TestLInsert)
{
	log_message("\n========TestLInsert========");
	string key, val, pivot;
	nemo::Position pos;
	int64_t llen;
	
	s_.OK();//Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空
	pos = nemo::AFTER;
	key = GetRandomKey_();
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, llen);
	if(s_.IsNotFound() && 0 == llen)
		log_success("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");
	else
		log_fail("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");

	s_.OK();//Key存在，且有list数据结构；
	val = GetRandomVal_();
	llen = 10;
	key = "LInsert_Test";
	write_list(llen, key);
	pivot = key+"_"+itoa(GetRandomUint_(0, llen-1));
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(11, llen);
	if(s_.ok() && llen == 11)
		log_success("Key存在，且有list数据结构");
	else
		log_fail("Key存在，且有list数据结构");

	s_.OK();//Key存在，且有list数据结构；但是没有pivot
	val = GetRandomVal_();
	llen = 10;
	key = "LInsert_Test";
	write_list(llen, key);
	pivot = key+"_"+itoa(llen+10);
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(-1, llen);
	if(s_.ok() && -1 == llen)
		log_success("Key存在，且有list数据结构；但是没有pivot");
	else
		log_fail("Key存在，且有list数据结构；但是没有pivot");
}

TEST_F(NemoListTest, TestLRem)
{
	log_message("\n========TestLRem========");
	string key, val;
	int64_t count, rem_count;

	s_.OK();//Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空
	key = GetRandomKey_();
	val = GetRandomVal_();
	rem_count = -1;
	s_ = n_->LRem(key, 0, val, &rem_count);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rem_count);
	if(s_.IsNotFound() && 0 == rem_count)
		log_success("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");
	else
		log_fail("Key不存在/Key存在，存的是不是List结构/Key存在，存的是List结构，但是list为空");

	s_.OK();//Key存在，且存的是list数据，count=0; ｜count｜< 有val的个数；
	int64_t llen, tempLLen;
	string tempVal;
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = 0;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key存在，且存的是list数据，count=0; ｜count｜< 有val的个数");
	else
		log_fail("Key存在，且存的是list数据，count=0; ｜count｜< 有val的个数");

	s_.OK();//Key存在，且存的是list数据，count<0; ｜count｜< 有val的个数；
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = GetRandomUint_(1, llen-1);
	count = (-1)*count;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, rem_count);
	if(s_.ok() && rem_count == (-1)*count)
		log_success("Key存在，且存的是list数据，count<0; ｜count｜< 有val的个数；");
	else
		log_fail("Key存在，且存的是list数据，count<0; ｜count｜< 有val的个数；");


	s_.OK();//Key存在，且存的是list数据，count<0; ｜count｜> 有val的个数；
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = llen+30;
	count = (-1)*count;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key存在，且存的是list数据，count<0; ｜count｜> 有val的个数");
	else
		log_fail("Key存在，且存的是list数据，count<0; ｜count｜> 有val的个数");


	s_.OK();//Key存在，且存的是list数据，count>0; ｜count｜< 有val的个数；
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = GetRandomUint_(1, llen-1);
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(count, rem_count);
	if(s_.ok() && count == rem_count)
		log_success("Key存在，且存的是list数据，count>0; ｜count｜< 有val的个数；");
	else
		log_fail("Key存在，且存的是list数据，count>0; ｜count｜< 有val的个数；");

	s_.OK();//Key存在，且存的是list数据，count>0; ｜count｜> 有val的个数；
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = llen+20;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key存在，且存的是list数据，count>0; ｜count｜> 有val的个数；");
	else
		log_fail("Key存在，且存的是list数据，count>0; ｜count｜> 有val的个数；");
	log_message("============================LISTTEST END===========================");
	log_message("============================LISTTEST END===========================\n\n");
}
