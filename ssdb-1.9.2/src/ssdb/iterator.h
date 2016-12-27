/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "../util/bytes.h"

#ifdef USE_LEVELDB
#include <leveldb/db.h>

namespace leveldb{
#else
#include <rocksdb/snapshot.h>

#define leveldb rocksdb
namespace rocksdb{
#endif
	class Iterator;
}

class Iterator{
public:
	enum Direction{
		FORWARD, BACKWARD
	};
	Iterator(leveldb::Iterator *it,
			const std::string &end,
			uint64_t limit,
			Direction direction=Iterator::FORWARD,
			const leveldb::Snapshot *snapshot=nullptr
	);
	~Iterator();
	bool skip(uint64_t offset);
	bool next();
	Bytes key();
	Bytes val();

	const leveldb::Snapshot *snapshot;
private:
	leveldb::Iterator *it;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};



class MIterator{
public:
	char dataType;
	std::string key;

	MIterator(Iterator *it);
	~MIterator();
	bool next();
private:
	Iterator *it;
};


// todo r2m adaptation
class HIterator{
public:
	std::string name;
	std::string key;
	std::string val;
	uint16_t 	version;

	HIterator(Iterator *it, const Bytes &name, uint16_t version = 0);
	~HIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class SIterator{
public:
	std::string name;
	std::string key;
	uint16_t 	version;

	SIterator(Iterator *it, const Bytes &name, uint16_t version = 0);
	~SIterator();
	bool next();

private:
	Iterator	*it;
};


// todo r2m adaptation
class ZIterator{
public:
	std::string name;
	std::string key;
	double      score;

	uint16_t 	version;

	ZIterator(Iterator *it, const Bytes &name, uint16_t version);
	~ZIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};

class LIterator{
public:
	std::string name;
	uint64_t    seq;
	uint16_t 	version;

	LIterator(Iterator *it, const Bytes &name, uint16_t version = 0);
	~LIterator();
	bool next();

private:
	Iterator	*it;
};



class EIterator{
public:
	std::string key;
	int64_t      score;

	EIterator(Iterator *it);
	~EIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};
#endif
