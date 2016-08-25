#include <iostream>
#include <string>
#include <vector>
#include <sys/time.h>
#include <unistd.h>
#include <sstream>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

#ifndef NEMO_TEST_H
#define NEMO_TEST_H

#define LOG_FILE_NAME "./test_report.txt"
#define LOG_FILE log_file
#define log_success(M, ...) fprintf(LOG_FILE, "[success] (%s:%d)" M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#define log_fail(M, ...) fprintf(LOG_FILE,    "[  FAIL ] (%s:%d)" M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#define log_message(M, ...) fprintf(LOG_FILE, M "\n", ##__VA_ARGS__)

#define eps 1e-8 //定义浮点数比较的精度
using namespace std;

inline string itoa(int32_t num)
{
	stringstream strm;
	strm << num;
	return strm.str();
}

class NemoTest : public ::testing::Test 
{
public:
	NemoTest(): n_(NULL)
	{
	}

	virtual ~NemoTest()
	{
	}

	virtual void SetUp()
	{
		//std::cout << "Enter the SetUp" << endl;
		nemo::Options options;
		options.target_file_size_base = 20*1024*1024;
		n_ = new nemo::Nemo(string("./tmp/"), options);
		s_.OK();
		LOG_FILE = fopen(LOG_FILE_NAME, "a+");
	}
	
	string GetRandomChars_(const unsigned int length)
	{
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		string randomStr;
		for(unsigned int index = 0; index != length; index++)
		{
				randomStr += charsSet[random()%charsSetLen_];
		}
		return randomStr;
	}

	string GetRandomBytes_(const unsigned int length)
	{
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		string randomBytes;
		for(unsigned int index = 0; index != length; index++)
		{
			randomBytes += char(random()%256);
		}
		return randomBytes;
	}

	unsigned int GetRandomUint_(unsigned int Min, unsigned int Max)
	{
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		return (random()%(Max+1-Min))+Min;
	}

	int64_t GetRandomInt64_(int64_t Min, int64_t Max) {
		assert(Max >= Min);
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		return (random()%(Max+1-Min))+Min;
	}

	double GetRandomFloat_(int64_t min = 0, int64_t max= 256 ) {//6位有效小数位数
		assert(max >= min);
		int base_precision = 1000000;
		int64_t min_ex = min * base_precision;
		int64_t max_ex = max * base_precision;
		int64_t res_int = GetRandomInt64_(min_ex, max_ex);
		return res_int*1.0/base_precision;
	}
	
	inline string GetRandomKey_()
	{
		return GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
	}
	inline string GetRandomField_()
	{
		return GetRandomBytes_(GetRandomUint_(minFieldLen_, maxFieldLen_));
	}
	inline string GetRandomVal_()
	{
		return GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
	}

	void GetRandomKeyValue_(string &key, string &val)
	{
		key = GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
		val = GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
	}
	
	rocksdb::Status SetSingleNormalKeyValue_(string &key, string &val)
	{
		key = GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
		val = GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
		return n_->Set(key, val);
	}

	rocksdb::Status SetMultiNormalKeyValue_(const uint32_t Num, vector<nemo::KV> *kvsPtr, vector<string> *keysPtr)
	{
		nemo::KV kv_temp;
		vector<nemo::KV> kvs;
		if(kvsPtr == NULL)
			kvsPtr = &kvs;
		string key, val;
		for(uint32_t index = 0; index != Num; index++)
		{
			//cout << "index:	" << index << endl;
			key = GetRandomBytes_(GetRandomUint_(minKeyLen_, maxKeyLen_));
			val = GetRandomBytes_(GetRandomUint_(minValLen_, maxValLen_));
			kv_temp.key = key;
			kv_temp.val = key;
			kvsPtr->push_back(kv_temp);
			if(keysPtr != NULL)
			{
				keysPtr->push_back(key);
			}
		}
		return n_->MSet(*kvsPtr);
	}
	
	bool isDoubleEqual(double d1, double d2) {
		double d_diff = d1 - d2;
		return (d_diff<eps) && (d_diff>-eps);
	}
	
	//Macro
	//#define CHECK_STATUS(state) EXPECT_STREQ(#state, s_.ToString().c_str())
	#define CHECK_STATUS(state) EXPECT_EQ((uint32_t)0, s_.ToString().find_first_of(#state))  //state Mostly be OK, NotFound, Corruption	
	#define STATUS_NOT(state) EXPECT_NE((uint32_t)0, s_.ToString().find_first_of(#state))
	#define STATE_NOT_OK_EXIT() if(!(s_.ok())) return;
	//Macro
	virtual void TearDown()
	{
		fclose(LOG_FILE);
		if(n_ != NULL)
			delete n_;
	}
protected:
	nemo::Nemo *n_;
	rocksdb::Status s_;
	FILE *LOG_FILE;
	static const unsigned int maxKeyLen_ = 254;
	static const unsigned int minKeyLen_ = 0;
	static const unsigned int maxFieldLen_ = 1024000;
	static const unsigned int minFieldLen_ = 0;
	static const unsigned int maxValLen_ = 1024000;
	static const unsigned int minValLen_ = 1;
	string charsSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"; //娌″暐鐢ㄤ簡
	static const unsigned int charsSetLen_ = 62;
};

#endif
