#include <iostream>
#include <string>
#include <vector>
#include <sys/time.h>
#include <unistd.h>
#include <sstream>

#include "gtest/gtest.h"

#ifndef SSDB_TEST_H
#define SSDB_TEST_H


#define eps 1e-5 //why 1e-8?
#define MAX_UINT64 ~(uint64_t)0
#define MAX_UINT32 ~(uint32_t)0
using namespace std;


inline string itoa(int32_t num)
{
	stringstream strm;
	strm << num;
	return strm.str();
}

class SSDBTest : public ::testing::Test 
{
public:
	SSDBTest()
	{
	}

	virtual ~SSDBTest()
	{
	}

	virtual void SetUp()
	{
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

	int64_t GetRandomUInt64_(uint64_t Min, uint64_t Max) {
		assert(Max >= Min);
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		return (random()%(Max+1-Min))+Min;
	}

	double GetRandomFloat_(int64_t min = 0, int64_t max= 256 ) {//6λ??ЧС??λ??
		assert(max >= min);
		int base_precision = 1000000;
		int64_t min_ex = min * base_precision;
		int64_t max_ex = max * base_precision;
		int64_t res_int = GetRandomInt64_(min_ex, max_ex);
		return res_int*1.0/base_precision;
	}
	
	inline unsigned int GetRandomSeq_()
	{
		return GetRandomUInt64_(minSeq, maxSeq);
	}

	inline unsigned int GetRandomVer_()
	{
		return GetRandomUint_(minVersion, maxVersion);
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
	
	
	bool isDoubleEqual(double d1, double d2) {
		double d_diff = d1 - d2;
		return (d_diff<eps) && (d_diff>-eps);
	}
	
	//Macro
	virtual void TearDown()
	{
	}
protected:
	static const uint64_t maxSeq = MAX_UINT64-1;
	static const uint64_t minSeq = 0;
	static const unsigned int maxVersion = 65535;
	static const unsigned int minVersion = 0;
	static const unsigned int maxKeyLen_ = 65535;
	static const unsigned int minKeyLen_ = 1;
	static const unsigned int maxMemberLen_ = 1024000;
	static const unsigned int minMemberLen_ = 0;
	static const unsigned int maxFieldLen_ = 1024000;
	static const unsigned int minFieldLen_ = 0;
	static const unsigned int maxValLen_ = 1024000;
	static const unsigned int minValLen_ = 1;
	static const unsigned int charsSetLen_ = 62;
};

#endif
