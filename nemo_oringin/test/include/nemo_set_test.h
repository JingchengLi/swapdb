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

#ifndef NEMO_SET_TEST_H
#define NEMO_SET_TEST_H

using namespace std;

class NemoSetTest : public NemoTest
{
public:
	bool isUnique(vector<string> &members) {
		map<string, int> tempMap;
		bool flag = true;
		for (vector<string>::iterator iter = members.begin(); iter != members.end(); iter++) {
			tempMap[*iter]++;
		}
		for (map<string, int>::iterator iter = tempMap.begin(); iter != tempMap.end(); iter++) {
			if (iter->second > 1) {
				flag = false;
				break;
			}
		}
		return flag;
	}
	
	bool isAllInKey(string key, vector<string> &members) { //for SRandMember test
		bool flag = true;
		for (vector<string>::iterator iter = members.begin(); iter != members.end(); iter++) {
			if (!(n_->SIsMember(key, *iter)))
			{
				flag = false;
				break;
			}
		}
		return flag;
	}


	void write_set(const string &key, const int64_t num)
	{
		int64_t resTemp;
		//numPre = n_->SCard(key);
		//for (int64_t index = 0; index != numPre; index++) {
		//	n_->SRem(key, key+"_"+itoa(index), &resTemp);
		//}
		nemo::SIterator* siter = n_->SScan(key, -1);
    for (; siter->Valid(); siter->Next())
		//while(siter->Next())
		{
			n_->SRem(key, siter->member(), &resTemp);
		}
		delete siter;
		for (int64_t index = 0; index != num; index++)
		{
			n_->SAdd(key, key+"_"+itoa(index), &resTemp);
		}
	}
	
	void write_set_scope(const string &key, const int64_t numStart, const int64_t numEnd)
	{
		int64_t resTemp; 
		//numPre = n_->SCard(key);
		//for (int64_t index = 0; index != numPre; index++) {
		//	n_->SRem(key, key+"_"+itoa(index), &resTemp);
		//}
		nemo::SIterator* siter = n_->SScan(key, -1);
    for (; siter->Valid(); siter->Next())
    {
		//while(siter->Next()) {
			n_->SRem(key, siter->member(), &resTemp);
		}
		delete siter;
		for (int64_t index = numStart; index != numEnd; index++)
		{
			n_->SAdd(key, itoa(index), &resTemp);
		}
	}

	void deleteAllMembers(string key) {
		nemo::SIterator* siter = n_->SScan(key, -1);
		int64_t resTemp;
		//while (siter->Next()) {
    for (; siter->Valid(); siter->Next()) {
			n_->SRem(key, siter->member(), &resTemp);
		}
		delete siter;
	}
};
#endif
