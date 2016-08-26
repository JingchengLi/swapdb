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

#ifndef NEMO_LIST_TEST_H
#define NEMO_LIST_TEST_H

using namespace std;

class NemoListTest : public NemoTest 
{
public:
	void write_list(int64_t llen, string key)
	{
		int64_t tempLLen, index;
		string getVal;
		n_->LLen(key, &tempLLen);
		while(tempLLen > 0)
		{
			n_->LPop(key, &getVal);	
			tempLLen--;
		}
		for(int64_t idx = 0; idx != llen; idx++)
		{
			n_->RPush(key, key + "_" + itoa(idx), &tempLLen);
		}
		log_message("===List from left to right is %s_0 to %s_%lld", key.c_str(), key.c_str(), llen);
	}
};

#endif
