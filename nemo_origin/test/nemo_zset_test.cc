#include <string>
#include <vector>
#include <sys/time.h>
#include <cstdlib>
#include <cstdlib>
#include <string>
#include <cmath>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

//#include "stdint.h"
#include "nemo_zset_test.h"
using namespace std;


TEST_F(NemoZSetTest, TestZAdd) {
	log_message("============================ZSETTEST START===========================");
	log_message("============================ZSETTEST START===========================");
	log_message("========TestZAdd========");

	string key, member;
	int64_t res;
	double score, scoreGet;

	s_.OK();//原来的key和member对应的值不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(score,scoreGet)) {
		log_success("原来的key和member对应的值不存在");
	} else {
		log_fail("原来的key和member对应的值不存在");
	}

	s_.OK();//原来的key和member对应的值存在
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 0 && isDoubleEqual(score, scoreGet)) {
		log_success("原来的key和member对应的值存在");
	} else {
		log_fail("原来的key和member对应的值存在");	
	}

	s_.OK();//原来的key和member对应的值存在；新score和原来的值在eps（10-5）范围内
	double scorePre = score;
	score = score+0.0000005;
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(scorePre, scoreGet));
	if (s_.ok() && res == 0 && isDoubleEqual(scorePre, scoreGet)) {
		log_success("原来的key和member对应的值存在；新score和原来的值在eps（10-5）范围内");
	} else {
		log_fail("原来的key和member对应的值存在；新score和原来的值在eps（10-5）范围内");
	}

	s_.OK();//key不为空；member为空
	key = GetRandomKey_();
	member = "";
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(score, scoreGet)) {
		log_success("key不为空；member为空");
	} else {
		log_fail("key不为空；member为空");
	}

	s_.OK();//key为空，member不为空
	key = "";
	member = GetRandomVal_();
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(score, scoreGet)) {
		log_success("key为空，member不为空");
	} else {
		log_fail("key为空，member不为空");
	}
}

TEST_F(NemoZSetTest, TestZCard) {
	log_message("\n========TestZCard========");

	string key, member;
	int64_t resTemp, card;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
	card = n_->ZCard(key);
	EXPECT_EQ(0, card);
	if (card == 0) {
		log_success("原来的key不存在");
	} else {
		log_fail("原来的key不存在");
	}

	s_.OK();//原来的key存在
	key = "ZCardTest";
	int64_t num = 10;
	write_zset_random_score(key, num);
	card = n_->ZCard(key);
	EXPECT_EQ(num, card);
	if (card == num) {
		log_success("原来的key存在");
	} else {
		log_fail("原来的key存在");
	}
}

TEST_F(NemoZSetTest, TestZCount) {
	log_message("\n========TestZCount========");

	string key, member;
	int64_t count, num, res;
	double begin, end, score;

	s_.OK();//key不存在/key存在，但没有zset结构
	key = GetRandomKey_();
	count = n_->ZCount(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX);
	EXPECT_EQ(0, count);
	if (count == 0) {
		log_success("key不存在/key存在，但没有zset结构");
	} else {
		log_fail("key不存在/key存在，但没有zset结构");
	}
	
	s_.OK();//key存在，且存的是zset结构，begin<end
	key = "ZCount_Test";
	num = 20;
	write_zset_up_score(key, 20);
	begin = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(begin), num-1);
	count = n_->ZCount(key, begin, end);
	EXPECT_EQ(floor(end)-ceil(begin)+1, count);
	if (floor(end)-ceil(begin)+1 == count) {
		log_success("floor(end)-ceil(begin)+1, count");
	} else {
		log_fail("floor(end)-ceil(begin)+1, count");
	}

	s_.OK();//key存在，且存的是zset结构，begin>end
	begin = GetRandomFloat_(1, num-2);
	end = GetRandomFloat_(0, begin-1);
	count = n_->ZCount(key, begin, end);
	EXPECT_EQ(0, count);
	if (count == 0) {
		log_success("key存在，且存的是zset结构，begin>end");
	} else {
		log_fail("key存在，且存的是zset结构，begin>end");
	}

	s_.OK();//key存在，且存的是zset结构，begin<end;测试is_lo=true是否起作用
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = 3.000012;
	n_->ZAdd(key, score, member, &res);
	member = GetRandomVal_();
	score = 3.000032;
	n_->ZAdd(key, score, member, &res);
	begin = 3.00001;
	end = ZSET_SCORE_MAX;
	count = n_->ZCount(key, begin, end, true);
	EXPECT_EQ(1, count);
	if (count == 1) {
		log_success("key存在，且存的是zset结构，begin<end;测试is_lo=true是否起作用");
	} else {
		log_fail("key存在，且存的是zset结构，begin<end;测试is_lo=true是否起作用");
	}

	s_.OK();//key存在，且存的是zset结构，begin<end;测试is_ro=true是否起作用
	begin = ZSET_SCORE_MIN;
	end = 3.00003;
	count = n_->ZCount(key, begin, end, false, true);
	EXPECT_EQ(1, count);
	if (1 == count) {
		log_success("key存在，且存的是zset结构，begin<end;测试is_ro=true是否起作用");
	} else {
		log_fail("key存在，且存的是zset结构，begin<end;测试is_ro=true是否起作用");
	}
}

TEST_F(NemoZSetTest, TestZScan) {
	log_message("\n========TestZScan========");

	string key, member;
	int64_t num, limit, count;
	double start, end;
	nemo::ZIterator* ziter;

	key = "ZScan_Test";
	num = 30;
	write_zset_up_score(key, num);
	
#define ZScanLoopProcess(countExpected, message)\
	ziter = n_->ZScan(key, start, end, limit);\
	count = 0;\
  for (; ziter->Valid(); ziter->Next())\
	{\
		count++;\
	}\
	EXPECT_EQ((countExpected), count);\
	if (count == (countExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)


	s_.OK();//start和end都在scores范围内（正常情况），limit=-1
	start = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(start), num-1);
	limit = -1;
	ZScanLoopProcess(floor(end)-ceil(start)+1, "start和end都在scores范围内（正常情况），limit=-1");

	s_.OK();//start和end都在scores范围内（正常情况），len>limit>0
	start = ZSET_SCORE_MIN;
	end = ZSET_SCORE_MAX;
	limit = GetRandomUint_(1, num-1);
	ZScanLoopProcess(limit, "start和end都在scores范围内（正常情况），len>limit>0");

	s_.OK();//start和end都在scores范围内（正常情况），limit>len>0
	start = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(start), num-1);
	limit = num+10;
	ZScanLoopProcess(floor(end)-ceil(start)+1, "start和end都在scores范围内（正常情况），limit>len>0");

	s_.OK();//start和end都在scores范围内（start>end）,limit=-1
	start = GetRandomFloat_(1, num-1);
	end = GetRandomFloat_(0, floor(start));
	limit = -1;
	ZScanLoopProcess(0,  "start和end都在scores范围内（start>end）,limit=-1");

	s_.OK();//start<scoreMin，end在scores范围内,limit=-1
	start = (-1)*GetRandomFloat_();
	end = GetRandomFloat_(0, num-1);
	limit = -1;
	ZScanLoopProcess(floor(end)+1, "start和end都在scores范围内（start>end）,limit=-1");

	s_.OK();//start在scores范围内；end为>scoreMax,limit=-1
	start = GetRandomFloat_(0, num-1);
	end = num+10;
	limit = -1;
	ZScanLoopProcess(num-ceil(start), "start在scores范围内；end为>scoreMax,limit=-1");

	s_.OK();//start,end都>scoreMax,且start>end,limit=-1
	start = num+10;
	end = num+5;
	limit = -1;
	ZScanLoopProcess(0, "start,end都>scoreMax,且start>end,limit=-1");

	s_.OK();//start,end都>scoreMax，start<end,limit=-1
	start = num+10;
	end = num+20;
	limit = -1;
	ZScanLoopProcess(0, "start,end都>scoreMax，start<end,limit=-1");

	s_.OK();//start,end都<scoreMin，start<end,limit=-1
	start = -10;
	end = -5;
	limit = -1;
	ZScanLoopProcess(0, "start,end都<scoreMin，start<end,limit=-1");

	s_.OK();//start,end都<scoreMin，start>end,limit=-1
	start = -10;
	end = -20;
	limit = -1;
	ZScanLoopProcess(0, "start,end都<scoreMin，start<end,limit=-1");

	s_.OK();//start<scoreMin<scoreMax<end,limit=-1
	start = -10;
	end = num+10;
	limit = -1;
	ZScanLoopProcess(num, "start<scoreMin<scoreMax<end,limit=-1");

	s_.OK();//end<scoreMin<scoreMax<start,limit=-1
	end = -10;
	start = num+10;
	limit = -1;
	ZScanLoopProcess(0, "end<scoreMin<scoreMax<start,limit=-1");
}

TEST_F(NemoZSetTest, TestZIncrby) {
	log_message("\n=========TestZIncrby========");
	string key, member, newVal;
	double score, by, scoreGet;
	int64_t res;


	s_.OK();//key和member对应的score不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	by = GetRandomFloat_();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_TRUE(isDoubleEqual(by, atof(newVal.c_str())));
	if (s_.ok() && isDoubleEqual(by, atof(newVal.c_str()))) {
		log_success("key和member对应的score不存在");
	} else {
		log_fail("key和member对应的score不存在");
	}

	s_.OK();//key和member对应的score存在
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_TRUE(isDoubleEqual(2*by, atof(newVal.c_str())));
	if (s_.ok() && isDoubleEqual(2*by, atof(newVal.c_str()))) {
		log_success("key和member对应的score存在");
	} else {
		log_fail("key和member对应的score存在");
	}

	s_.OK();//key和member对应的score不存在,by超限制（正负10000000000000LL）
	key = GetRandomKey_();
	member = GetRandomVal_();
	by = ZSET_SCORE_MAX*1.5;
	newVal.clear();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_TRUE(newVal.empty());
	if (s_.IsCorruption() && newVal.empty()) {
		log_success("key和member对应的score不存在,by超限制（正负10000000000000LL）");
	} else {
		log_fail("key和member对应的score不存在,by超限制（正负10000000000000LL）");
	}

	s_.OK();//key和member对应的score存在,加by后超限制（正负10000000000000LL）
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = ZSET_SCORE_MIN+100;
	n_->ZAdd(key, score, member, &res);
	by = -1000.0f;
	newVal.clear();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_TRUE(newVal.empty());
	if(s_.IsCorruption() && newVal.empty()) {
		log_success("key和member对应的score存在,by超限制（正负10000000000000LL）");
	} else {
		log_fail("key和member对应的score存在,by超限制（正负10000000000000LL）");
	}
}

TEST_F(NemoZSetTest, TestZRange) {
	log_message("\n========TestZRange========");
	string key, member;
	int64_t start, stop, num;
	vector<nemo::SM> sms;

#define ZRangeLoopProcess(sizeExpected, message)\
	sms.clear();\
	s_ = n_->ZRange(key, start, stop, sms);\
	CHECK_STATUS(OK);\
	EXPECT_EQ((sizeExpected), sms.size());\
	if(s_.ok() && sms.size() == (sizeExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)


	s_.OK();//key不存在/key存在，但没有zset结构
	key = GetRandomKey_();
	member = GetRandomVal_();
	start = 0; 
	stop = GetRandomUint_(0, 256);
	ZRangeLoopProcess(0, "key存在;0<end<start<len");

	num = 50;
	key = "ZRange_Test";
	write_zset_random_score(key, num);

	s_.OK();//key存在;0<=start<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	ZRangeLoopProcess(stop-start+1, "key存在;0<=start<end<=len-1");

	s_.OK();//key存在;-len<start<0;0<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	start = start-num;
	ZRangeLoopProcess(stop-start-num+1, "key存在;-len<start<0;0<end<=len-1");

	s_.OK();//key存在;start<-len;0<end<=len-1
	start = (num+5)*(-1);
	stop = GetRandomUint_(0, num-1);
	ZRangeLoopProcess(stop+1, "key存在;start<-len;0<end<=len-1");

	s_.OK();//key存在;start>=len;0<end<=len-1
	start = num+11;
	stop = GetRandomUint_(0, num-1);
	ZRangeLoopProcess(0, "key存在;start>=len;0<end<=len-1");

	s_.OK();//key存在;0<start<=len-1;end<-len
	start = GetRandomUint_(0, num-1);
	stop = (-1)*(num+12);
	ZRangeLoopProcess(0, "key存在;0<start<=len-1;end<-len");

	s_.OK();//key存在;0<start<=len-1;-len<=end<0
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	stop = stop-num;
	ZRangeLoopProcess(stop+num-start+1, "key存在;0<start<=len-1;-len<=end<0");

	s_.OK();//key存在;0<start<=len-1;end>=len
	start = GetRandomUint_(0, num-1);
	stop = num+20;
	ZRangeLoopProcess(num-start, "key存在;0<start<=len-1;end>=len");

	s_.OK();//key存在;0<end<start<len
	stop = GetRandomUint_(0, num-2);
	start = GetRandomUint_(stop+1, num-1);
	ZRangeLoopProcess(0, "key存在;0<end<start<len");
}

TEST_F(NemoZSetTest, TestZUnionStore) {
	log_message("\n========TestZUnionStore========");
	string destination, key1, key2, key3;
	int64_t res, zcard, num1, num2, num3;
	int num_keys;
	nemo::Aggregate agg;
	vector<string> keys;
	vector<double> weights;
	double scoreGet;

	s_.OK();//destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；
	destination = GetRandomKey_();
	num_keys = 0;
	agg = SUM;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(0, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；numkeys=0；keys为单个key；weights默认；agg=sum；
	key1 = "ZUnionStoreSrcKey1";
	keys.push_back(key1);
	num1 = 10;
	write_zset_up_score_scope(key1, 0, 10);
	destination = "ZUnionStoreDstKey1";
	num_keys = 0;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(0, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination不存在；numkeys=0；keys为单个key；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys=0；keys为单个key；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；
	num_keys = 1;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(10, zcard);
	if(s_.ok() && res == 10 && zcard == 10) {
		log_success("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	}

	/*
	s_.OK();//destination不存在；numkeys>1；keys为单个key；weights默认；agg=sum；
	num_keys = 2;
	cout << "keys.size():" << keys.size() << endl;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(10, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination不存在；numkeys>1；keys为单个key；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys>1；keys为单个key；weights默认；agg=sum；");
	}
	*/

	s_.OK();//destination不存在；numkeys=size；keys为多个key，没有重复元素；weights默认；agg=sum；
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 0, 20);
	key2 = "ZUnionStoreSrcKey2";
	write_zset_up_score_scope(key2, 20, 40);
	keys.clear();
	keys.push_back(key1);
	keys.push_back(key2);
	destination = "ZUnionStoreDstKey2";
	num_keys = 2;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	if (s_.ok() && res == 40) {
		log_success("destination不存在；numkeys=size；keys为多个key，没有重复元素；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys=size；keys为多个key，没有重复元素；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=sum；判断sum是否重合起作用
	keys.clear();
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 0, 20);
	key2 = "ZUnionStoreSrcKey2";
	write_zset_up_score_scope(key2, 19, 39);
	keys.push_back(key1);
	keys.push_back(key2);
	destination = "ZUnionStoreDstKey3";
	num_keys = 2;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(2*19, scoreGet));
	if (s_.ok() && res == 39 && isDoubleEqual(2*19, scoreGet)) {
		log_success("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=sum；判断sum是否重合起作用");
	} else {
		log_fail("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=sum；判断sum是否重合起作用");
	}

	s_.OK();//destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断max是否重合起作用
	agg = MAX;
	string temp;
	n_->ZIncrby(key2, itoa(19), 10.0, temp);
	destination = "ZUnionStoreDstKey4";
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(29.0, scoreGet));
	if (s_.ok() && res == 39 && isDoubleEqual(29.0, scoreGet)) {
		log_success("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断max是否重合起作用");
	} else {
		log_fail("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断max是否重合起作用");
	}

	s_.OK();//destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断min是否重合起作用
	agg = MIN;
	destination = "ZUnionStoreDstKey5";
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(19.0, scoreGet));
	if(s_.ok() && res == 39 && isDoubleEqual(19.0, scoreGet)) {
		log_success("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断min是否重合起作用");
	} else {
		log_fail("destination不存在；numkeys=size；keys为多个key，有重复元素；weights默认；agg=max；判断min是否重合起作用");
	}

	s_.OK();//destination存在；numkeys=1；keys为单个key；weights默认；agg=sum；看destination的原来内容是否清除
	destination = "ZUnionStoreDstKey6";
	write_zset_up_score_scope(destination, 0, 10);
	keys.clear();
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 10, 20);
	keys.push_back(key1);
	num_keys = 1;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	nemo::Status s_tmp = n_->ZScore(destination, itoa(1), &scoreGet);
	EXPECT_TRUE(s_tmp.IsNotFound());
	if (s_.ok() && res == 10 && s_tmp.IsNotFound()) {
		log_success("destination存在；numkeys=1；keys为单个key；weights默认；agg=sum；看destination的原来内容是否清除");
	} else {
		log_fail("destination存在；numkeys=1；keys为单个key；weights默认；agg=sum；看destination的原来内容是否清除");
	}
}

TEST_F(NemoZSetTest, TestZInterStore) {
	log_message("\n========TestZInterStore========");
	string destination, key1, key2, key3;
	int64_t res, zcard, num1, num2, num3;
	int num_keys;
	nemo::Aggregate agg;
	vector<string> keys;
	vector<double> weights;
	double scoreGet;

	s_.OK();//destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；
	destination =  "ZInterStore_Test_Dst1";
	keys.clear();
	key1 = "ZInterStore_Test_Src1";
	keys.push_back(key1);
	write_zset_up_score_scope(key1, 0, 10);
	num_keys = 1;
	agg = SUM;
	res = 0;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；numkeys=1；keys为单个key；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；keys为两个key；没有重复元素，weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst2";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src2";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src3";
	write_zset_up_score_scope(key2, 10, 20);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("destination不存在；keys为两个key；没有重复元素，weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；keys为两个key；没有重复元素，weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；keys为两个key；有重复元素，weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst3";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src4";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src5";
	write_zset_up_score_scope(key2, 9, 19);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(2*9.0,scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(2*9.0,scoreGet)) {
		log_success("destination不存在；keys为两个key；有重复元素，weights默认；agg=sum");
	} else {
		log_fail("destination不存在；keys为两个key；有重复元素，weights默认；agg=sum");
	}

	s_.OK();//destination不存在；keys为三个key；没有重复元素；weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst4";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src6";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src7";
	write_zset_up_score_scope(key2, 10, 20);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src8";
	write_zset_up_score_scope(key3, 20, 30);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && 0 == res) {
		log_success("destination不存在；keys为三个key；没有重复元素；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；keys为三个key；没有重复元素；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；keys为三个key；只有两两重复元素，没有全部重复的元素；weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst5";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src9";
	write_zset_up_score_scope(key1, 0, 30);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src10";
	write_zset_up_score_scope(key2, 20, 50);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src11";
	write_zset_up_score_scope(key3, 40, 70);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && 0 == res) {
		log_success("destination不存在；keys为三个key；只有两两重复元素，没有全部重复的元素；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；keys为三个key；只有两两重复元素，没有全部重复的元素；weights默认；agg=sum；");
	}

	s_.OK();//destination不存在；keys为三个key；有全部重复的元素；weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst6";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src12";
	write_zset_up_score_scope(key1, 0, 30);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src13";
	write_zset_up_score_scope(key2, 20, 50);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src14";
	write_zset_up_score_scope(key3, 29, 70);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(29), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(3*29.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(3*29.0, scoreGet)) {
		log_success("destination不存在；keys为三个key；有全部重复的元素；weights默认；agg=sum；");
	} else {
		log_fail("destination不存在；keys为三个key；有全部重复的元素；weights默认；agg=sum；");
	}
	
	s_.OK();//destination不存在；keys为两个key；有重复元素，weights默认；agg=max；
	destination = "ZInterStore_Test_Dst7";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src15";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src16";
	write_zset_up_score_scope(key2, 9, 40);
	string temp;
	n_->ZIncrby(key2, itoa(9), 11, temp);
	keys.push_back(key2);
	agg = MAX;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(20.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(20.0, scoreGet)) {
		log_success("destination不存在；keys为两个key；有重复元素，weights默认；agg=max");
	} else {
		log_fail("destination不存在；keys为两个key；有重复元素，weights默认；agg=max");
	}

	s_.OK();//destination不存在；keys为两个key；有重复元素，weights默认；agg=min
	agg = MIN;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(9.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(9.0, scoreGet)) {
		log_success("destination不存在；keys为两个key；有重复元素，weights默认；agg=min");
	} else {
		log_fail("destination不存在；keys为两个key；有重复元素，weights默认；agg=min");
	}

	s_.OK();//destination存在；keys为两个key；没有重复元素，weights默认；agg=sum；
	destination = "ZInterStore_Test_Dst8";
	write_zset_up_score_scope(destination, 0, 10);
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src17";
	write_zset_up_score_scope(key1, 10, 20);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src18";
	write_zset_up_score_scope(key2, 20, 30);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	nemo::Status s_temp = n_->ZScore(destination, itoa(1), &scoreGet);
	EXPECT_TRUE(s_temp.IsNotFound());
	if (s_.ok() && res == 0 && s_temp.IsNotFound()) {
		log_success("destination存在；keys为两个key；没有重复元素，weights默认；agg=sum");
	} else {
		log_fail("destination存在；keys为两个key；没有重复元素，weights默认；agg=sum");
	}
}

TEST_F(NemoZSetTest, TestZRangebyscore) {
	log_message("\n========TestZRangebyscore========");
	string key;
	double start, stop, offset;
	int64_t num;
	vector<nemo::SM> sms;

	key = "ZRangebyscore_Test";
	num = 100;
	write_zset_up_score(key, num);

#define ZRangebyscoreLoopProcess(sizeExpected, message)\
	sms.clear();\
	s_ = n_->ZRangebyscore(key, start, stop, sms);\
	CHECK_STATUS(OK);\
	EXPECT_EQ((sizeExpected), sms.size());\
	if (s_.ok() && sms.size() == (sizeExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)

	s_.OK();//scoreMin<start<end<scoreMax
	start = GetRandomFloat_(0, num-2);
	stop = GetRandomFloat_(ceil(start), num-1);
	ZRangebyscoreLoopProcess(floor(stop)-ceil(start)+1, "scoreMin<start<end<scoreMax");

	s_.OK();//scoreMin<end<start<scoreMax
	stop = GetRandomFloat_(0, num-3);
	start = GetRandomFloat_(ceil(stop)+1, num-1);
	ZRangebyscoreLoopProcess(0, "scoreMin<end<start<scoreMax");

	s_.OK();//start<scoreMin，end在scores范围内
	start = -1;
	stop = GetRandomFloat_(0, num-1);
	ZRangebyscoreLoopProcess(floor(stop)+1, "start<scoreMin，end在scores范围内");

	s_.OK();//start在scores范围内；end为>scoreMax
	start = GetRandomFloat_(0, num-1);
	stop = num+1;
	ZRangebyscoreLoopProcess(num-ceil(start), "start在scores范围内；end为>scoreMax,");

	s_.OK();//start,end都>scoreMax,且start>end
	start = num+10;
	stop = num+5;
	ZRangebyscoreLoopProcess(0, "start,end都>scoreMax,且start>end");

	s_.OK();//start,end都>scoreMax，start<end
	start = num+6;
	stop = num+23;
	ZRangebyscoreLoopProcess(0, "start,end都>scoreMax，start<end");

	s_.OK();//start,end都<scoreMin，start<end
	start = -10;
	stop = -1;
	ZRangebyscoreLoopProcess(0, "start,end都<scoreMin，start<end");

	s_.OK();//start,end都<scoreMin，start>end
	start = -2;
	stop = -23;
	ZRangebyscoreLoopProcess(0, "start,end都<scoreMin，start>end");

	s_.OK();//start<scoreMin<scoreMax<end
	start = -2;
	stop = num+1;
	ZRangebyscoreLoopProcess(num, "start<scoreMin<scoreMax<end");

	s_.OK();//end<scoreMin<scoreMax<start
	stop = -3;
	start = num+10;
	ZRangebyscoreLoopProcess(0, "end<scoreMin<scoreMax<start");
}

TEST_F(NemoZSetTest, TestZRem) {
	log_message("\n========TestZRem========");
	string key, member;
	double score;
	int64_t res;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	res = -1;
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("原来的key不存在");
	} else {
		log_fail("原来的key不存在");
	}

	s_.OK();//原来的key存在，存的不是ZSet结构数据
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->Set(key, member);
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0,res);
	if (s_.IsNotFound() && res == 0) {
		log_success("原来的key存在，存的不是ZSet结构数据");
	} else {
		log_fail("原来的key存在，存的不是ZSet结构数据");
	}

	s_.OK();//原来的key存在，存的是ZSet的数据结构，不包含member
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	member = GetRandomVal_();
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("原来的key存在，存的是ZSet的数据结构，不包含member");
	} else {
		log_fail("原来的key存在，存的是ZSet的数据结构，不包含member");
	}

	s_.OK();//原来的key存在，存的是ZSet的数据结构，包含member
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("原来的key存在，存的是ZSet的数据结构，包含member");
	} else {
		log_fail("原来的key存在，存的是ZSet的数据结构，包含member");
	}
}

TEST_F(NemoZSetTest, TestZRank) {
	log_message("\n========TestZRank========");
	string key, member;
	int64_t rank, num, memberInt;

	s_.OK();//原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是zset结构，但是没有member成员
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZRank(key, member, &rank);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rank);
	if (s_.IsNotFound() && rank == 0) {
		log_success("原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是zset结构，但是没有member成员");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是zset结构，但是没有member成员");
	}

	s_.OK();//原来的key存的是zset结构，而且有member成员
	key = "ZRank_Test";
	num = 50;
	write_zset_up_score(key, num);
	memberInt = GetRandomUint_(0, num-1);
	s_ = n_->ZRank(key, itoa(memberInt), &rank);
	CHECK_STATUS(OK);
	EXPECT_EQ(memberInt, rank);
	if (s_.ok() && memberInt == rank) {
		log_success("原来的key存的是zset结构，而且有member成员");
	} else {
		log_fail("原来的key存的是zset结构，而且有member成员");
	}
}

TEST_F(NemoZSetTest, TestZRevrank) {
	log_message("\n========TestZRevrank========");
	string key, member;
	int64_t rank, num, memberInt;

	s_.OK();//原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是set结构，但是没有member成员
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZRevrank(key, member, &rank);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rank);
	if (s_.IsNotFound() && rank == 0) {
		log_success("原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是zset结构，但是没有member成员");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是zset结构/原来的key存的是zset结构，但是没有member成员");
	}

	s_.OK();//原来的key存的是set结构，而且有member成员
	key = "ZRevrank_Test";
	num = 50;
	write_zset_up_score(key, num);
	memberInt = GetRandomUint_(0, num-1);
	s_ = n_->ZRevrank(key, itoa(memberInt), &rank);
	CHECK_STATUS(OK);
	EXPECT_EQ((num-1-memberInt), rank);
	if (s_.ok() && (num-1-memberInt) == rank) {
		log_success("原来的key存的是zset结构，而且有member成员");
	} else {
		log_fail("原来的key存的是zset结构，而且有member成员");
	}
}

TEST_F(NemoZSetTest, TestZScore) {
	log_message("\n========TestZScore========");
	string key, member;
	int64_t res;
	double score, scoreGet;

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && scoreGet == 0) {
		log_success("原来的key不存在");
	} else {
		log_fail("原来的key不存在");
	}

	s_.OK();//原来的key存在，存的不是zset结构
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && scoreGet == 0) {
		log_success("原来的key存在，存的不是zset结构");
	} else {
		log_fail("原来的key存在，存的不是zset结构");
	}
	
	s_.OK();//原来的key存的是zset结构,而且有member成员
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(OK);
	EXPECT_EQ(score, scoreGet);
	if (s_.ok() && score == scoreGet) {
		log_success("原来的key存的是zset结构,而且有member成员");
	} else {
		log_fail("原来的key存的是zset结构,而且有member成员");
	}

	s_.OK();//原来的key存的是set结构，但是没有member成员
	member = GetRandomVal_();
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && 0 == scoreGet) {
		log_success("原来的key存的是set结构，但是没有member成员");
	} else {
		log_fail("原来的key存的是set结构，但是没有member成员");
	}
}

TEST_F(NemoZSetTest, TestZRangelex) {
	log_message("\n========TestZRangelex========");
	string key, member, min, max;
	uint32_t minInt, maxInt, num;
	vector<string> members;

	
	s_.OK();//原来的key不存在/原来的key存在，存的不是set结构
	key = GetRandomKey_();
	min = "";
	max = "";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_TRUE(member.empty());
	if (s_.ok() && members.empty()) {
		log_success("原来的key不存在/原来的key存在，存的不是set结构");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是set结构");
	}

	key = "ZRangebylex_Test";
	num = 10;
	write_zset_up_score(key, num);

	s_.OK();//原来的key存在，max与min和members没有交集
	min = "a";
	max = "z";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_TRUE(members.empty());
	if (s_.ok() && members.empty()) {
		log_success("原来的key存在，max与min和members没有交集");
	} else {
		log_fail("原来的key存在，max与min和members没有交集");
	}

	s_.OK();//原来的key存在，max与min和members有交集，且没有相同的score
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(minInt+1, num-1);
	members.clear();
	s_ = n_->ZRangebylex(key, itoa(minInt), itoa(maxInt), members);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, members.size());
	if (s_.ok() && maxInt-minInt+1==members.size()) {
		log_success("原来的key存在，max与min和members有交集，且没有相同的score");
	} else {
		log_fail("原来的key存在，max与min和members有交集，且没有相同的score");
	}

	s_.OK();//原来的key存在，max和min位空
	min = "";
	max = "";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	if (s_.ok() && num == members.size()) {
		log_success("原来的key存在，max和min与members有交集，且offset>0");
	} else {
		log_fail("原来的key存在，max和min与members有交集，且offset>0");
	}

	s_.OK();//原来的key存在，max和min与scores有交集，且offset>0
	min = "";
	max = "";
//	uint32_t offset = GetRandomUint_(0, num-1);	
//	members.clear();
//	s_ = n_->ZRangebylex(key, min, max, members, offset);
//	CHECK_STATUS(OK);
//	EXPECT_EQ(num-offset, members.size());
//	if (s_.ok() && num-offset == members.size()) {
//		log_success("原来的key存在，max和min与members有交集，且offset>0");
//	} else {
//		log_fail("原来的key存在，max和min与members有交集，且offset>0");
//	}
	
	s_.OK();//原来的key存在，max与min和scores有交集，且有相同的score
	min = "";
	max = "";
	write_zset_same_score(key, num);
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	EXPECT_TRUE(isSorted(members));
	if (s_.ok() && num == members.size() && isSorted(members)) {
		log_success("原来的key存在，max与min和members有交集，且有相同的score");
	} else {
		log_fail("原来的key存在，max与min和members有交集，且有相同的score");
	}
}

TEST_F(NemoZSetTest, TestZLexcount) {
	log_message("\n========TestZLexcount========");
	string key, min, max;
	int64_t count, num, numPre, minInt, maxInt;

	s_.OK();//原来的key不存在/原来的key存在，存的不是set结构
	key = GetRandomKey_();
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && 0 == count) {
		log_success("原来的key不存在/原来的key存在，存的不是set结构");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是set结构");
	}

	numPre = 10000;
	num = 100;
	key = "ZLexcount_Test";
	write_zset_random_score(key, num, numPre);

	s_.OK();//原来的key存在，max与min和members没有交集
	minInt = num + 10;
	maxInt = num + 23;
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("原来的key存在，max与min和members没有交集");
	} else {
		log_fail("原来的key存在，max与min和members没有交集");
	}

	s_.OK();//原来的key存在，max与min和members有交集
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(num-2, num-1);
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, count);
	if (s_.ok() && count == maxInt-minInt+1) {
		log_success("原来的key存在，max与min和members有交集");
	} else {
		log_fail("原来的key存在，max与min和members有交集");
	}

	s_.OK();//原来的key存在，max与min和为空
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("原来的key存在，max与min和为空");
	} else {
		log_fail("原来的key存在，max与min和为空");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebylex) {
	log_message("\n========TestZRemrangbylex========");
	string key, min, max;
	int64_t count, minInt, maxInt, num, numPre;

	s_.OK();//原来的key不存在/原来的key存在，存的不是set结构
	key = GetRandomKey_();
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && 0 == count) {
		log_success("原来的key不存在/原来的key存在，存的不是set结构");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是set结构");
	}

	numPre = 10000;
	num = 100;
	key = "ZLexcount_Test";
	write_zset_random_score(key, num, numPre);

	s_.OK();//原来的key存在，max与min和members没有交集
	minInt = num + 23;
	maxInt = num + 43;
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("原来的key存在，max与min和members没有交集");
	} else {
		log_fail("原来的key存在，max与min和members没有交集");
	}

	s_.OK();//原来的key存在，max与min和members有交集
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(minInt+1, num-1);
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, count);
	if (s_.ok() && count == maxInt-minInt+1) {
		log_success("原来的key存在，max与min和members有交集");
	} else {
		log_fail("原来的key存在，max与min和members有交集");
	}


	s_.OK();//原来的key存在，max与min和为空
	write_zset_random_score(key, num, numPre);
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("原来的key存在，max与min和为空");
	} else {
		log_fail("原来的key存在，max与min和为空");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebyrank) {
	log_message("\n========TestZRemrangebyrank========");
	string key;
	int64_t start, stop, count, num;

	s_.OK();//原来的key不存在/原来的key存在，存的不是set结构
	key = GetRandomKey_();
	start = 0;
	stop = 100;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("原来的key不存在/原来的key存在，存的不是set结构");
	} else {
		log_fail("原来的key不存在/原来的key存在，存的不是set结构");
	}

	key = "ZRemrangebyrank_Test";
	num = 100;
	write_zset_up_score(key, num);
	s_.OK();//key存在;0<=start<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop-start+1, count);
	if(s_.ok() && count == stop-start+1) {
		log_success("key存在;0<=start<end<=len-1");
	} else {
		log_fail("key存在;0<=start<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;-len<start<0;0<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	start = start-num;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop-start-num+1, count);
	if(s_.ok() && count == stop-start-num+1) {
		log_success("key存在;-len<start<0;0<end<=len-1");
	} else {
		log_fail("key存在;-len<start<0;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;start<-len;0<end<=len-1
	start = (-1)*(num+4);
	stop = GetRandomUint_(0, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop+1, count);
	if(s_.ok() && count == stop+1) {
		log_success("key存在;start<-len;0<end<=len-1");
	} else {
		log_fail("key存在;start<-len;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;start>=len;0<end<=len-1
	start = num+23;
	stop = GetRandomUint_(start, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key存在;start>=len;0<end<=len-1");
	} else {
		log_fail("key存在;start>=len;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;0<start<=len-1;end<-len
	start = GetRandomUint_(0, num-1);
	stop = (-1)*(num+12);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key存在;0<start<=len-1;end<-len");
	} else {
		log_fail("key存在;0<start<=len-1;end<-len");
	}
	
	write_zset_up_score(key, num);
	s_.OK();//key存在;0<start<=len-1;-len<=end<0
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	stop = stop - num;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop+num-start+1, count);
	if(s_.ok() && count == stop+num-start+1) {
		log_success("key存在;0<start<=len-1;-len<=end<0");
	} else {
		log_fail("key存在;0<start<=len-1;-len<=end<0");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;0<start<=len-1;end>=len
	start = GetRandomUint_(0, num-1);
	stop = num+17;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num-start, count);
	if(s_.ok() && count == num-start) {
		log_success("key存在;0<start<=len-1;end>=len");
	} else {
		log_fail("key存在;0<start<=len-1;end>=len");
	}

	write_zset_up_score(key, num);
	s_.OK();//key存在;0<end<start<len
	stop = GetRandomUint_(0, num-2);
	start = GetRandomUint_(stop+1, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key存在;0<end<start<len");
	} else {
		log_fail("key存在;0<end<start<len");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebyscore) {
	log_message("\n========TestZRemrangebyscore========");

	string key;
	double start, stop;
	int64_t count, num;

	key = "ZRemrangebyrank_Test";
	num = 100;

	write_zset_up_score(key, num);
	s_.OK();//key非空；scoreMin<start<end<scoreMax
	start = GetRandomFloat_(0, num-2);
	stop = GetRandomFloat_(ceil(start), num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(floor(stop)-ceil(start)+1, count);
	if (s_.ok() && count == floor(stop)-ceil(start)+1) {
		log_success("key非空；scoreMin<start<end<scoreMax");
	} else {
		log_fail("key非空；scoreMin<start<end<scoreMax");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；scoreMin<end<start<scoreMax
	stop = GetRandomFloat_(0, num-2);
	start = GetRandomFloat_(ceil(stop), num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；scoreMin<end<start<scoreMax");
	} else {
		log_fail("key非空；scoreMin<end<start<scoreMax");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start<scoreMin，end在scores范围内
	start = -1;
	stop = GetRandomFloat_(0, num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(floor(stop)+1, count);
	if (s_.ok() && count == floor(stop)+1) {
		log_success("key非空；start<scoreMin，end在scores范围内");
	} else {
		log_fail("key非空；start<scoreMin，end在scores范围内");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start在scores范围内；end为>scoreMax,
	start = GetRandomFloat_(0, num-2);
	stop = num+14;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num-ceil(start), count);
	if (s_.ok() && count == num-ceil(start)) {
		log_success("key非空；start在scores范围内；end为>scoreMax,");
	} else {
		log_fail("key非空；start在scores范围内；end为>scoreMax,");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start,end都>scoreMax,且start>end
	start = num+5;
	stop = num+3;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；start,end都>scoreMax,且start>end");
	} else {
		log_fail("key非空；start,end都>scoreMax,且start>end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start,end都>scoreMax，start<end
	start = num+7;
	stop = num+10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；start,end都>scoreMax，start<end");
	} else {
		log_fail("key非空；start,end都>scoreMax，start<end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start,end都<scoreMin，start<end
	start = -8;
	stop = -2;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；start,end都<scoreMin，start<end");
	} else {
		log_fail("key非空；start,end都<scoreMin，start<end");
	}
	
	write_zset_up_score(key, num);
	s_.OK();//key非空；start,end都<scoreMin，start>end
	start = -3;
	stop = -23;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；start,end都<scoreMin，start>end");
	} else {
		log_fail("key非空；start,end都<scoreMin，start>end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；start<scoreMin<scoreMax<end
	start = -7;
	stop = num+10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("key非空；start<scoreMin<scoreMax<end");
	} else {
		log_fail("key非空；start<scoreMin<scoreMax<end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key非空；end<scoreMin<scoreMax<start
	start = num+13;
	stop = -10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key非空；end<scoreMin<scoreMax<start");
	} else {
		log_fail("key非空；end<scoreMin<scoreMax<start");
	}
	log_message("============================ZSETTEST END===========================");
	log_message("============================ZSETTEST END===========================");
}
