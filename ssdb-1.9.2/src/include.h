/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_INCLUDE_H_
#define SSDB_INCLUDE_H_

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "version.h"

#ifndef UINT64_MAX
	#define UINT64_MAX		18446744073709551615ULL
#endif
#ifndef INT64_MAX
	#define INT64_MAX		0x7fffffffffffffffLL
#endif


const int ZSET_SCORE_INTEGER_DIGITS = 13;
const int ZSET_SCORE_DECIMAL_DIGITS = 5;
//const int64_t ZSET_SCORE_SHIFT = 1000000000000000000LL; //1,000,000,000,000,000,000
const int64_t ZSET_SCORE_MAX = 10000000000000LL;               //10,000,000,000,000 ( *10,000 for Shift )
const int64_t ZSET_SCORE_MIN = -ZSET_SCORE_MAX;
const double eps = 1e-5;

static inline double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}

static inline int64_t time_ms(){
	struct timeval now;
	gettimeofday(&now, NULL);
	return (int64_t)now.tv_sec * 1000 + (int64_t)now.tv_usec/1000;
}

#endif

