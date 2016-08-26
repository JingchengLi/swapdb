#include <iostream>
#include <string>
#include <vector>
#include <sys/time.h>
#include <unistd.h>
#include <sstream>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"
#include "nemo_test.h"

#ifndef NEMO_KV_TEST_H
#define NEMO_KV_TEST_H

using namespace std;

class NemoKVTest : public NemoTest 
{
public:	
	nemo::KV GetKV(const string &key, const string &val)
	{
		nemo::KV kv;
		kv.key = key;
		kv.val = val;
		return kv;
	}
	
protected:
	static const unsigned int minMSetNum_ = 0;
	static const unsigned int maxMSetNum_ = 100;
	static const unsigned int maxMDelNum_ = 300;
};

#endif
