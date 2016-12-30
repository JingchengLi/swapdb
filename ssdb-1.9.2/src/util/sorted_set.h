/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_SORTED_SET_H
#define UTIL_SORTED_SET_H

#include <inttypes.h>
#include <string>
#include <map>
#include <set>

template <typename T>
class Item
{
public:
	std::string key;
	T score;

	bool operator<(const Item& b) const{
		return this->score < b.score
			   || (this->score == b.score && this->key < b.key);
	}
};

template <typename T>
class SortedSet
{
public:
	bool empty() const{
		return this->size() == 0;
	}

	int add(const std::string &key, T score){
		int ret;
		typename std::map<std::string, typename std::set<Item<T>>::iterator>::iterator it;

		it = existed.find(key);
		if(it == existed.end()){
			// new item
			ret = 1;
		}else{
			ret = 0;
			typename std::set<Item<T>>::iterator it2 = it->second;
			const Item<T> &item = *it2;
			if(item.score == score){
				// not updated
				return 0;
			}
			// remove existing item
			sorted_set.erase(it2);
		}

		Item<T> item;
		item.key = key;
		item.score = score;

		std::pair<typename std::set<Item<T>>::iterator, bool> p = sorted_set.insert(item);
		existed[key] = p.first;

		return ret;
	}


	int size() const{
		return (int)sorted_set.size();
	}

	int del(const std::string &key){
		int ret;
		typename std::map<std::string, typename std::set<Item<T>>::iterator>::iterator it;

		it = existed.find(key);
		if(it == existed.end()){
			// new item
			ret = 0;
		}else{
			ret = 1;
			sorted_set.erase(it->second);
			existed.erase(it);
		}
		return ret;
	}

	int front(std::string *key, T *score) const{
		typename std::set<Item<T>>::iterator it2 = sorted_set.begin();
		if(it2 == sorted_set.end()){
			return 0;
		}
		const Item<T> &item = *it2;
		*key = item.key;
		if(score){
			*score = item.score;
		}
		return 1;
	}

	int back(std::string *key, T *score) const{
		typename std::set<Item<T>>::reverse_iterator it2 = sorted_set.rbegin();
		if(it2 == sorted_set.rend()){
			return 0;
		}
		const Item<T> &item = *it2;
		*key = item.key;
		if(score){
			*score = item.score;
		}
		return 1;
	}

	T max_score() const{
		T score = 0;
		std::string key;
		this->back(&key, &score);
		return score;
	}

	int pop_front(){
		if(sorted_set.empty()){
			return 0;
		}
		typename std::set<Item<T>>::iterator it = sorted_set.begin();
		const Item<T> &item = *it;
		existed.erase(item.key);
		sorted_set.erase(it);
		return 1;
	}

	int pop_back(){
		if(sorted_set.empty()){
			return 0;
		}
		typename std::set<Item<T>>::iterator it = sorted_set.end();
		it --;
		const Item<T> &item = *it;
		existed.erase(item.key);
		sorted_set.erase(it);
		return 1;
	}


private:

	std::map<std::string, typename std::set<Item<T>>::iterator> existed;
	std::set<Item<T>> sorted_set;

};


/*
TODO: HashedWheel
Each item is linked in two list, one is slot list, the other
one is total list.
*/
/*
template <class T>
class SortedList
{
public:
	void add(const T data, int64_t score);
	T front();
	void pop_front();

	class Item
	{
	public:
		int64_t score;
		Item *prev;
		Item *next;
		//Item *slot_prev;
		//Item *slot_next;
		T data;
	};
};
*/

#endif
