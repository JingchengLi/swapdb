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
#include "nemo_set_test.h"
using namespace std;


TEST_F(NemoSetTest, TestSAdd) {
	log_message("============================SETTEST START===========================");
	log_message("============================SETTEST START===========================");
	log_message("========TestSAdd========");
	string key, member, getMember;
	int64_t res;
	
	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	deleteAllMembers(key);
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);
	if (s_.ok() && 1 == res && member == getMember) {
		log_success("原来的key不存在");
	} else {
		log_fail("原来的key不存在");
	}
	
	s_.OK();//原来的key存在，存的不是Set结构数据
	key = GetRandomKey_();
	deleteAllMembers(key);
	member = GetRandomVal_();
	n_->Set(key, member);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);	
	if (s_.ok() && 1 == res && member == getMember) {
		log_success("原来的key存在，存的不是Set结构数据");
	} else {
		log_fail("原来的key存在，存的不是Set结构数据");
	}

	s_.OK();//原来的key存在，存的是Set的数据结构，包含member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("原来的key存在，存的是Set的数据结构，不包含member（正常情况）");
	} else {
		log_fail("原来的key存在，存的是Set的数据结构，不包含member（正常情况）");
	}

	s_.OK();//原来的key存在，存的是Set的数据结构，不包含member（正常情况）
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	res = 0;
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("原来的key存在，存的是Set的数据结构，不包含member（正常情况）");
	} else {
		log_fail("原来的key存在，存的是Set的数据结构，不包含member（正常情况）");
	}

	s_.OK();//member取val的最大长度，其他正常情况
	member = GetRandomBytes_(maxValLen_);
	deleteAllMembers(key);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);
  printf ("s_ is %s, res = %d, member.size()=%d getMember.size()=%d  %d\n", s_.ToString().c_str(), res, 
          member.size(), getMember.size(), member == getMember);
	if (s_.ok() && res == 1 && member == getMember) {
		log_success("member取key的最大值，其他正常情况");
	} else {
		log_fail("member取key的最大值，其他正常情况");
	}

	s_.OK();//member取空值，其他正常情况
	member = "";
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("member取空值，其他正常情况");
	} else {
		log_fail("member取空值，其他正常情况");
	}

	s_.OK();//key取空值
	key = "";
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && 1 == res) {
		log_success("key取空值");
	} else {
		log_fail("key取空值");
	}
	n_->SRem(key, member, &res);
}

TEST_F(NemoSetTest, TestSRem) {
	log_message("\n========TestSRem========");
	string key, member;
	int64_t res;

#define SRemLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SRem(key, member, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && expectedRes == res) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if(string(#expectedState) == "NotFound") {\
		if(s_.IsNotFound() && expectedRes == res) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do{} while(false)
	

	s_.OK();//原来的key不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	SRemLoopProcess(NotFound, 0, "原来的key不存在");
	
	s_.OK();//原来的key存在，存的不是Set结构数据
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->Set(key, member);
	SRemLoopProcess(NotFound, 0, "原来的key存在，存的不是Set结构数据");

	s_.OK();//原来的key存在，存的是Set的数据结构，不包含member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	member = GetRandomVal_();
	res = -1;
	SRemLoopProcess(NotFound, 0, "原来的key存在，存的是Set的数据结构，不包含member");

	s_.OK();//原来的key存在，存的是Set的数据结构，包含member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	res = -1;
	SRemLoopProcess(OK, 1, "原来的key存在，存的是Set的数据结构，包含member");
}

TEST_F(NemoSetTest, TestSCard) {
	log_message("\n========TestSRem========");

	string key, member;
	int64_t res, card;

	s_.OK();//原来的key存在／原来的key存在，存的不是Set结构数据
	key = GetRandomKey_();
	card = n_->SCard(key);
	EXPECT_EQ(0, card);
	if (0 == card) {
		log_success("原来的key存在／原来的key存在，存的不是Set结构数据");
	} else {
		log_fail("原来的key存在／原来的key存在，存的不是Set结构数据");
	}

	s_.OK();//原来的key存在，存的是Set的数据结构
	int64_t num = 13;
	key = "SCard_Test";
	for(int index = 0; index != num; index++) {
		n_->SAdd(key, key+"_"+itoa(index), &res);
	}
	card = n_->SCard(key);
	EXPECT_EQ(num, card);
	if (num == card) {
		log_success("原来的key存在，存的是Set的数据结构");
	} else {
		log_fail("原来的key存在，存的是Set的数据结构");
	}
}

TEST_F(NemoSetTest, TestSScan) {
	log_message("\n========TestSScan========");

	string key, member;
	nemo::SIterator* siter;
	int64_t num, limit;
	
#define SScanLoopProcess(limit, expectedNum, testMessage)\
	siter = n_->SScan(key, limit);\
	num = 0;\
  for (; siter->Valid(); siter->Next()){\
		num++;\
	}\
	EXPECT_EQ(expectedNum, num);\
	if (expectedNum == num) {\
		log_success(testMessage);\
	} else {\
		log_fail(testMessage);\
	}\
	delete siter


	s_.OK();//原来的key不存在；limit＝－1
	key = GetRandomKey_();
	limit = -1;
	SScanLoopProcess(limit, 0, "原来的key不存在；limit＝－1");

	s_.OK();//key为空值；limint＝－1
	key = "";//前面已经删除了key为空值的情况，这里就不删除了
	limit = -1; 
	SScanLoopProcess(limit, 0, "key为空值；limint＝－1");

	s_.OK();//原来的key存在，存的不是Set结构数据；limit＝－1
	key = GetRandomKey_();
	member = GetRandomVal_();
	limit = -1;
	SScanLoopProcess(limit, 0, "原来的key存在，存的不是Set结构数据；limit＝－1");

	s_.OK();//原来的key存在；存的是Set数据结构；limit＝－1
	num = 12;
	key = "SScan_Test";
	write_set(key, num);
	limit = -1;
	SScanLoopProcess(limit, num, "原来的key存在；存的是Set数据结构；limit＝－1");

	s_.OK();//原来的key存在；存的是Set数据结构；limit为正数；小于key集合的元素个数
	limit = 10;
	SScanLoopProcess(limit, limit, "原来的key存在；存的是Set数据结构；limit为正数；小于key集合的元素个数");

	s_.OK();//原来的key存在；存的是Set数据结构；limit为正数；大于key集合的元素个数
	limit = 30;
	SScanLoopProcess(limit, num, "原来的key存在；存的是Set数据结构；limit为正数；大于key集合的元素个数");
}

TEST_F(NemoSetTest, TestSMembers) {
	log_message("\n========TestSScan========");
	
	string key, member;
	vector<string> members;
	uint64_t num;

	s_.OK();//原来的key不存在／原来的key存在，存的不是Set结构数据
	key = GetRandomKey_();
	members.clear();
	s_ = n_->SMembers(key, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("原来的key不存在／原来的key存在，存的不是Set结构数据");
	} else {
		log_fail("原来的key不存在／原来的key存在，存的不是Set结构数据");
	}

	s_.OK();//
	key = "SMembers_Test";
	num = 34;
	write_set(key, num);
	members.clear();
	s_ = n_->SMembers(key, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	if (s_.ok() && num == members.size()) {
		log_success("原来的key不存在／原来的key存在，存的不是Set结构数据");
	} else {
		log_fail("原来的key不存在／原来的key存在，存的不是Set结构数据");
	}
	
	members.clear();
}

TEST_F(NemoSetTest, TestSUnionStore) {
	log_message("\n========TestSUnionStore========");

	string keyDst, member, keyTemp;
	vector<string> keys;
	int64_t numStart, numEnd, res;
	
	s_.OK();//destination不存在；keys存在，保存的元素不重合
	keyDst = "SUnionStore_Test_Dst";
	keyTemp = "SUnionStore_Test_Src1";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src2";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	if (s_.ok() && 40 == res) {
		log_success("destination不存在；keys存在，保存的元素不重合");
	} else {
		log_fail("destination不存在；keys存在，保存的元素不重合");
	}

	s_.OK();//destination不存在；keys存在，保存的元素有重合
	keyDst = "SUnionStore_Test_Dst1";
	keys.clear();
	keyTemp = "SUnionStore_Test_Src3";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src4";
	numStart = 10;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(30, res);
	if (s_.ok() && 30 == res) {
		log_success("destination不存在；keys存在，保存的元素有重合");
	} else {
		log_fail("destination不存在；keys存在，保存的元素有重合");
	}

	s_.OK();//destination存在，且有Set元素；keys保存的元素不重合
	keys.clear();
	keyTemp = "SUnionStore_Test_Src5";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src6";
	numStart = 60;
	numEnd = 80;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	bool flagTemp = n_->SIsMember(keyDst, "0");
	EXPECT_EQ(false, flagTemp);//"SUnionStore_Test_Src3_0"保存在上一次的keyDst里
	if (s_.ok() && 40==res && !flagTemp) {
		log_success("destination存在，且有Set元素；keys保存的元素不重合");
	} else {
		log_fail("destination存在，且有Set元素；keys保存的元素不重合");
	}

	keys.clear();
}

TEST_F(NemoSetTest, TestSUnion) {
	log_message("\n========TestSUnion========");

	string member, keyTemp;
	vector<string> keys, members;
	int64_t numStart, numEnd;

	s_.OK();//keys里有不存在的key／或者存在，但是存的不是Set结构数据
	keyTemp = GetRandomKey_();
	keys.push_back(keyTemp);
	keyTemp = GetRandomKey_();
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("keys里有不存在的key／或者存在，但是存的不是Set结构数据");
	} else {
		log_fail("keys里有不存在的key／或者存在，但是存的不是Set结构数据");
	}

	s_.OK();//destination不存在；keys存在，保存的元素不重合
	keyTemp = "SUnion_Test_Src1";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnion_Test_Src2";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, members.size());
	if (s_.ok() && (uint64_t)(40) == members.size()) {
		log_success("destination不存在；keys存在，保存的元素不重合");
	} else {
		log_fail("destination不存在；keys存在，保存的元素不重合");
	}

	s_.OK();//destination不存在；keys存在，保存的元素有重合
	keys.clear();
	keyTemp = "SUnion_Test_Src3";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnion_Test_Src4";
	numStart = 10;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(30, members.size());
	if (s_.ok() && 30 == members.size()) {
		log_success("destination不存在；keys存在，保存的元素有重合");
	} else {
		log_fail("destination不存在；keys存在，保存的元素有重合");
	}

	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSInterStore) {
	log_message("\n========TestSInterStore========");
	
	string keyTemp, keyDst; 
	vector<string> keys;
	int64_t res, numStart, numEnd, num;
	
#define SInterStoreLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SInterStore(keyDst, keys, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination不存在，keys为空；
	keyDst = "SInterStore_Test_Dst1";
	keys.clear();
	res = -1;
	SInterStoreLoopProcess(Corruption, -1, "destination不存在，keys为空；");

	s_.OK();//destination不存在，keys个数为1
	keyDst = "SInterStore_Test_Dst2";
	keyTemp = "SInterStore_Test_Src1";
	num = 13;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, num, "destination不存在，keys个数为1");

	s_.OK();//destination不存在，keys个数为2，有重复的元素；
	keys.clear();
	keyDst = "SInterStore_Test_Dst3";
	keyTemp = "SInterStore_Test_Src2";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 10, "destination不存在，keys个数为2，有重复的元素；");

	s_.OK();//destination不存在，keys个数为2；没有重复的元素
	keys.clear();
	keyDst = "SInterStore_Test_Dst4";
	keyTemp = "SInterStore_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src5";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 0, "destination不存在，keys个数为2；没有重复的元素");

	s_.OK();//destination不存在，keys个数>=3；有重复的元素
	keys.clear();
	keyDst = "SInterStore_Test_Dst5";
	keyTemp = "SInterStore_Test_Src6";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src8";
	numStart = 10;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 10, "destination不存在，keys个数>=3；有重复的元素");

	s_.OK();//destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素
	keys.clear();
	keyDst = "SInterStore_Test_Dst6";
	keyTemp = "SInterStore_Test_Src9";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src11";
	numStart = 40;
	numEnd = 70;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 0, "destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素");

	s_.OK();//destination存在；keys个数为2；有重合的元素
	keyDst = "SInterStore_Test_Dst7";
	num = 10;
	write_set(keyDst, num);
	keys.clear();
	keyTemp = "SInterStore_Test_Src12";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src13";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SInterStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(20, res);
	bool flag = n_->SIsMember(keyDst, keyDst+"_0");
	EXPECT_EQ(false, flag);
	if (s_.ok() && 20 == res && !flag) {
		log_success("destination存在；keys个数为2；有重合的元素");
	} else {
		log_success("destination存在；keys个数为2；有重合的元素");
	}

	keys.clear();
}

TEST_F(NemoSetTest, TestSInter) {
	log_message("\n========TestSInter========");

	string keyTemp;
	vector<string> members;
	vector<string> keys;
	int64_t numStart, numEnd, num;
	
#define SInterLoopProcess(expectedState, expectedSize, testMessage)\
	s_ = n_->SInter(keys, members);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedSize, members.size());\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination不存在，keys为空；
	keys.clear();
	members.clear();
	SInterLoopProcess(Corruption, 0, "destination不存在，keys为空；");

	s_.OK();//destination不存在，keys个数为1
	keyTemp = "SInter_Test_Src1";
	num = 13;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, num, "destination不存在，keys个数为1");

	s_.OK();//destination不存在，keys个数为2，有重复的元素；
	keys.clear();
	keyTemp = "SInter_Test_Src2";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 10, "destination不存在，keys个数为2，有重复的元素；");

	s_.OK();//destination不存在，keys个数为2；没有重复的元素
	keys.clear();
	keyTemp = "SInter_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src5";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 0, "destination不存在，keys个数为2；没有重复的元素");

	s_.OK();//destination不存在，keys个数>=3；有重复的元素
	keys.clear();
	keyTemp = "SInter_Test_Src6";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src8";
	numStart = 10;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 10, "destination不存在，keys个数>=3；有重复的元素");

	s_.OK();//destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素
	keys.clear();
	keyTemp = "SInter_Test_Src9";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src11";
	numStart = 40;
	numEnd = 70;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 0, "destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素");
	
	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSDiffStore) {
	log_message("\n========TestSDiffStore========");
	
	string keyTemp, keyDst; 
	vector<string> keys;
	int64_t res, numStart, numEnd, num;
	
#define SDiffStoreLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SDiffStore(keyDst, keys, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination不存在，keys为空；
	keyDst = "SDiffStore_Test_Dst1";
	keys.clear();
	res = -1;
	SDiffStoreLoopProcess(Corruption, -1, "destination不存在，keys为空；");

	s_.OK();//destination不存在，keys个数为1
	keyDst = "SDiffStore_Test_Dst2";
	keyTemp = "SDiffStore_Test_Src1";
	num = 23;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, num, "destination不存在，keys个数为1");

	s_.OK();//destination不存在，keys个数为2，有不重复的元素；
	keys.clear();
	keyDst = "SDiffStore_Test_Dst3";
	keyTemp = "SDiffStore_Test_Src2";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 10, "destination不存在，keys个数为2，有不重复的元素；");

	s_.OK();//destination不存在，keys个数为2；没有不重复的元素
	keys.clear();
	keyDst = "SDiffStore_Test_Dst4";
	keyTemp = "SDiffStore_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src5";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 0, "destination不存在，keys个数为2；没有重复的元素");

	s_.OK();//destination不存在，keys个数>=3；两两之间全都不相互重复
	keys.clear();
	keyDst = "SDiffStore_Test_Dst5";
	keyTemp = "SDiffStore_Test_Src6";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src8";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 20, "destination不存在，keys个数>=3；有重复的元素");

	s_.OK();//destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素
	keys.clear();
	keyDst = "SDiffStore_Test_Dst6";
	keyTemp = "SDiffStore_Test_Src9";
	numStart = 0;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src11";
	numStart = 10;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 20, "destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素");

	s_.OK();//destination存在；keys个数为2；有重合的元素
	keyDst = "SDiffStore_Test_Dst7";
	num = 10;
	write_set(keyDst, num);
	keys.clear();
	keyTemp = "SDiffStore_Test_Src12";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src13";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SDiffStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	bool flag = n_->SIsMember(keyDst, keyDst+"_0");
	EXPECT_EQ(false, flag);
	if (s_.ok() && 10 == res && !flag) {
		log_success("destination存在；keys个数为2；有重合的元素");
	} else {
		log_success("destination存在；keys个数为2；有重合的元素");
	}
	
	keys.clear();
}

TEST_F(NemoSetTest, TestSDiff) {
	log_message("\n========TestSDiff========");
	
	string keyTemp;
	vector<string> keys, members;
	int64_t numStart, numEnd, num;
	
#define SDiffLoopProcess(expectedState, expectedSize, testMessage)\
	s_ = n_->SDiff(keys, members);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedSize, members.size());\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination不存在，keys为空；
	keys.clear();
	members.clear();
	SDiffLoopProcess(Corruption, 0, "destination不存在，keys为空；");

	s_.OK();//destination不存在，keys个数为1
	keyTemp = "SInterDiff_Test_Src1";
	num = 27;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, num, "destination不存在，keys个数为1");

	s_.OK();//destination不存在，keys个数为2，有不重复的元素；
	keys.clear();
	keyTemp = "SDiff_Test_Src2";
	numStart = 0;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src3";
	numStart = 10;
	numEnd = 49;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 10, "destination不存在，keys个数为2，有不重复的元素；");

	s_.OK();//destination不存在，keys个数为2；没有不重复的元素
	keys.clear();
	keyTemp = "SDiff_Test_Src4";
	numStart = 0;
	numEnd = 29;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src5";
	numStart = 0;
	numEnd = 29;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 0, "destination不存在，keys个数为2；没有重复的元素");

	s_.OK();//destination不存在，keys个数>=3；两两之间全都不相互重复
	keys.clear();
	keyTemp = "SDiff_Test_Src6";
	numStart = 0;
	numEnd = 17;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src8";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 17, "destination不存在，keys个数>=3；有重复的元素");

	s_.OK();//destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素
	keys.clear();
	keyTemp = "SDiff_Test_Src9";
	numStart = 0;
	numEnd = 49;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src11";
	numStart = 10;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 10, "destination不存在，keys个数大于>=3；有个别相互重复的元素但没有全部重复的元素");

	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSIsMember) {
	log_message("\n========TestSDiff========");
	
	string key, member;
	int64_t res;
	bool retBool;

	s_.OK();//key不存在
	key = GetRandomKey_();
	member = GetRandomVal_();
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(false, retBool);
	if (false == retBool) {
		log_success("key不存在");
	} else {
		log_fail("key不存在");
	}

	s_.OK();//key存的Set数据结构,且包含member
	n_->SAdd(key, member, &res);
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(true, retBool);
	if (true == retBool) {
		log_success("key存的Set数据结构,且包含member");
	} else {
		log_fail("key存的Set数据结构,且包含member");
	}

	s_.OK();//"key存的Set数据结构不包含member"
	member = GetRandomVal_();
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(false, retBool);
	if (false == retBool) {
		log_success("key存的Set数据结构不包含member");
	} else {
		log_fail("key存的Set数据结构不包含member");
	}
}

TEST_F(NemoSetTest, TestSPop) {
	log_message("\n========TestSPop========");

	string key, member, getMember;
	int64_t num, card, resTemp;
	bool flag;

	s_.OK();//key不存在／key存在，但是存的不是Set结构
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->SPop(key, getMember);
	CHECK_STATUS(NotFound);
	if (s_.IsNotFound()) {
		log_success("key不存在／key存在，但是存的不是Set结构");
	} else {
		log_fail("key不存在／key存在，但是存的不是Set结构");
	}

	s_.OK();//key存在，存的是Set结构
	key = GetRandomKey_();
	nemo::SIterator* siter = n_->SScan(key, -1);
  for (; siter->Valid(); siter->Next()) {
	//while (siter->Next()) {
		n_->SRem(key, siter->member(), &resTemp);
	}
	delete siter;
	member = GetRandomVal_();
	n_->SAdd(key, member, &resTemp);
	s_ = n_->SPop(key, getMember);
	CHECK_STATUS(OK);
	EXPECT_EQ(member, getMember);
	if (s_.ok() && member == getMember) {
		log_success("key存在，存的是Set结构");
	} else {
		log_fail("key存在，存的是Set结构");
	}

	s_.OK();//key存在；存的是Set结构，看是否可以SPop完
	key = "SPop_Test";
	num = 10;
	write_set(key, num);
	flag = true;
	while (num>0) {
		s_ = n_->SPop(key, getMember);
		if (!s_.ok()) {
			flag = false;
			break;
		}
		num--;
	}
	card = n_->SCard(key);
	EXPECT_EQ(true, flag);
	EXPECT_EQ(0, card);
	if (flag && 0 == card) {
		log_success("key存在；存的是Set结构，看是否可以SPop完");
	} else {
		log_fail("key存在；存的是Set结构，看是否可以SPop完");
	}
}

TEST_F(NemoSetTest, TestSRandMember) {
	log_message("========SRandMember========");

	string key;
	vector<string> members;
	int64_t count, card, num;
	bool isUniqueFlag, isAllInKeyFlag;

	s_.OK();//key为空／key存在，但是存的不是Set结构
	members.clear();
	key = GetRandomKey_();
	s_ = n_->SRandMember(key, members);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, members.empty());
	if (s_.IsNotFound() && members.empty()) {
		log_success("key为空／key存在，但是存的不是Set结构");
	} else {
		log_fail("key为空／key存在，但是存的不是Set结构");
	}

	key = "SRandMember_Test";
	num = 50;
	write_set(key, num);

	s_.OK();//key存在，count＝0；
	count = 0;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("key存在，count＝0");
	} else {
		log_fail("key存在，count＝0");
	}

	s_.OK();//key存在，count>card>0
	count = GetRandomUint_(1, num-1);
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(count, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && count == members.size() && isUniqueFlag && isAllInKeyFlag) {
		log_success("key存在，count>card>0");
	} else {
		log_fail("key存在，count>card>0");
	}
	
	s_.OK();//key存在，0<count<card
	count = num+13;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && num == members.size() && isUniqueFlag && isAllInKeyFlag) {
		log_success("key存在，count>card>0");
	} else {
		log_fail("key存在，count>card>0");
	}
	
	s_.OK();//key存在，－card<count<0
	count = GetRandomUint_(1, num-1);
	count = (-1)*count;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, members.size());
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && (-1)*count == members.size() && isAllInKeyFlag) {
		log_success("key存在，－card<count<0");
	} else {
		log_fail("key存在，－card<count<0");
	}

	s_.OK();//key存在，count<-card
	count = num+20;
	count = (-1)*count;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(false, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && (-1)*count == members.size() && (!isUniqueFlag) && isAllInKeyFlag) {
		log_success("key存在，count<-card");
	} else {
		log_fail("key存在，count<-card");
	}
}

TEST_F(NemoSetTest, TestSMove) {
	log_message("\n========TestSMove========");
	
	string keySrc, keyDst, member;
	int64_t res, resTemp, cardSrc, cardDst, numStart, numEnd;

	s_.OK();//srcKey不存在
	keySrc = GetRandomKey_();
	keyDst = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("srcKey不存在");
	} else {
		log_fail("srcKey不存在");
	}

	s_.OK();//srcKey存在，但是没有member
	n_->SAdd(keySrc, member, &resTemp);
	n_->SRem(keySrc, member, &resTemp);
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("srcKey存在，但是没有member");
	} else {
		log_fail("srcKey存在，但是没有member");
	}

	s_.OK();//srcKey存在，且有member；dstKey不存在
	keySrc = GetRandomKey_();
	member = GetRandomVal_();
	keyDst = GetRandomKey_();
	n_->SAdd(keySrc, member, &resTemp);
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(0, cardSrc);
	EXPECT_EQ(1, cardDst);
	if (s_.ok() && res==1 && 0==cardSrc && 1==cardDst) {
		log_success("srcKey存在，且有member；dstKey不存在");
	} else {
		log_fail("srcKey存在，且有member；dstKey不存在");
	}

	s_.OK();//srcKey存在，且有member；dstKey存在，但是没有member
	keySrc = "SMove_Test_Src1";
	numStart = 0;
	numEnd = 10;
	write_set_scope(keySrc, numStart, numEnd);
	keyDst = "SMove_Test_Dst1";
	numStart = 10;
	numEnd = 20;
	write_set_scope(keyDst, numStart, numEnd);
	s_ = n_->SMove(keySrc, keyDst, itoa(GetRandomUint_(0, 9)), &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(9, cardSrc);
	EXPECT_EQ(11, cardDst);
	if (s_.ok() && res==1 && 9==cardSrc&& 11==cardDst) {
		log_success("srcKey存在，且有member；dstKey存在，但是没有member");
	} else {
		log_fail("srcKey存在，且有member；dstKey存在，但是没有member");
	}

	s_.OK();//srdKey存在，且有member；dst存在，且有member
	keySrc = "SMove_Test_Src2";
	keyDst = "SMove_Test_Dst2";
	numStart = 10;
	numEnd = 20;
	write_set_scope(keySrc, numStart, numEnd);
	write_set_scope(keyDst, numStart, numEnd);
	s_ = n_->SMove(keySrc, keyDst, itoa(GetRandomUint_(numStart, numEnd-1)), &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(9, cardSrc);
	EXPECT_EQ(10, cardDst);
	if (s_.ok() && res==1 && 9 == cardSrc && 10 == cardDst) {
		log_success("srdKey存在，且有member；dst存在，且有member");
	} else {
		log_fail("srdKey存在，且有member；dst存在，且有member");
	}
	log_message("============================SETTEST END===========================");
	log_message("============================SETTEST END===========================\n\n");
}
