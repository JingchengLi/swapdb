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

#ifndef NEMO_HASH_TEST_H
#define NEMO_HASH_TEST_H

using namespace std;

class NemoHashTest : public NemoTest 
{
protected:
    static const unsigned int maxHMSetNum_ = 100;
	static const unsigned int maxHMGetNum_ = 200;
};

#endif
