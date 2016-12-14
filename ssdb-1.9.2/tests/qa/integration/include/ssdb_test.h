#include <iostream>
#include <string>
#include <vector>
#include <sys/time.h>
#include <unistd.h>
#include <sstream>
#include <math.h>
#include "SSDB_client.h"

#include "gtest/gtest.h"

#ifndef SSDB_TEST_H
#define SSDB_TEST_H


#define eps 1e-5 //why 1e-8?
#define MAX_UINT64 ~(uint64_t)0
#define MAX_UINT32 ~(uint32_t)0
#define MIN_INT64 (((int64_t)1) << 63 )
#define MAX_INT64 ~(MIN_INT64)
using namespace std;

#ifdef ROBUST
#define multi 1024
#else
#define multi 1
#endif
inline string itoa(int32_t num)
{
	stringstream strm;
	strm << num;
	return strm.str();
}

inline string i64toa(int64_t num)
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
        Keys = {
            "", "0", "1", "10", "123", "4321", "1234567890"
            "a", "ab", "cba", "abcdefghijklmnopqrstuvwxyz",
            "A", "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "a0b1", "A0aB9", "~!@#$%^&*()",
            "-`_+=|';:.,/?<>'`", "0{a}1", "00{aa}2{55}",
            "99{{1111}lll", "key_normal_{214}_gsdg"
        };
	}

	virtual ~SSDBTest()
	{
	}

    virtual void SetUp(){
        port = 8888;
        ip = "127.0.0.1";
        client = ssdb::Client::connect(ip.data(), port);
        ASSERT_TRUE(client != NULL)<<"fail to connect to server!";
    }

    virtual void TearDown(){
        delete client;
    }

	string GetRandomBytes_(const unsigned int length)
	{
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		string randomBytes;
		for(unsigned int index = 0; index != length; index++)
		{
			randomBytes += char(random()%95+32);//visible bytes
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

	int64_t GetRandomInt64_() {
        int64_t randomInt64 = 0;
		struct timeval currentTime;
        int bits = rand()%64;
        for(int count = 0; count < bits; count++)
        {
            gettimeofday(&currentTime, NULL);
            srand(currentTime.tv_usec);
            randomInt64 = ( randomInt64<<1 ) | ( rand()&1 );
        }
		return randomInt64;
	}

	uint64_t GetRandomUInt64_(uint64_t Min, uint64_t Max) {
		assert(Max >= Min);
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
		return (random()%(Max+1-Min))+Min;
	}

	double GetRandomDouble_() {
		/* int base_precision = 1000000;
		int64_t res_int = GetRandomInt64_();
		return res_int*1.0/base_precision; */
		struct timeval currentTime;
		gettimeofday(&currentTime, NULL);
		srand(currentTime.tv_usec);
        int eNum = rand()%24 - 12;
        float base = rand()*1.0/rand();
        double randomDouble = base*pow(10, eNum);
        return randomDouble; 
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
	
protected:
	static const uint64_t maxSeq = MAX_UINT64-1;
	static const uint64_t minSeq = 0;
	static const unsigned int maxVersion = 65535;
	static const unsigned int minVersion = 0;
	static const unsigned int maxKeyLen_ = 65535;
	static const unsigned int minKeyLen_ = 1;
	static const unsigned int maxMemberLen_ = 1024*multi;
	static const unsigned int minMemberLen_ = 0;
	static const unsigned int maxFieldLen_ = 1024*multi;
	static const unsigned int minFieldLen_ = 0;
	static const unsigned int maxValLen_ = 1024*multi;
	static const unsigned int minValLen_ = 1;
	static const unsigned int charsSetLen_ = 62;
    std::vector<std::string> Keys;
    ssdb::Client *client;
    string ip;
    int port;
};

#endif
