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

#ifndef NEMO_ZSET_TEST_H
#define NEMO_ZSET_TEST_H

using namespace std;

#define ZSET_SCORE_MIN nemo::ZSET_SCORE_MIN
#define ZSET_SCORE_MAX nemo::ZSET_SCORE_MAX

#define SUM nemo::SUM
#define MIN nemo::MIN
#define MAX nemo::MAX



class NemoZSetTest : public NemoTest
{
public:
	void write_zset_random_score(string key, int64_t num, int64_t numPre = 0) {
		string member;
		double score;
		int64_t resTemp;
		n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
		for (int64_t index = 0; index != num; index++) {
			score = GetRandomFloat_();
			member = itoa(index+numPre);
			n_->ZAdd(key, score, member, &resTemp);
		}
	}

	void write_zset_same_score(string key, int64_t num, double score = 1.0, int64_t numPre = 0) {
		string member;
		int64_t resTemp;
		n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
		for (int64_t index = 0; index != num; index++) {
			member = itoa(index+numPre);
			n_->ZAdd(key, score, member, &resTemp);
		}
	}

	void write_zset_up_score(string key, int64_t num, int64_t startScore = 0, int64_t numPre = 0) {
		string member;
		int64_t resTemp;
		double score;
		n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
		for (int64_t index = 0; index != num; index++) {
			member = itoa(index+numPre);
			score = index*1.0 + startScore;
			n_->ZAdd(key, score, member, &resTemp);
		}
	}

	void write_zset_up_score_scope(string key, int64_t start, int64_t end, int64_t startScore = 0, int64_t numPre = 0) {
		string member;
		int64_t resTemp;
		double score;
		n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
		for (int64_t index = start; index != end; index++) {
			member = itoa(index);
			score = index*1.0 + startScore;
			n_->ZAdd(key, score, member, &resTemp);
		}
	}

	bool isSorted(const vector<string> &members) {
		bool flag = true;
		string memberPre = "deadbeaf";
		for (vector<string>::const_iterator iter = members.begin(); iter != members.end(); iter++) {
			if (memberPre != "deadbeaf" && iter->compare(memberPre) < 0) {
				flag = false;
				break;
			}
			memberPre = *iter;
		}
		return flag;
	}
};
#endif
