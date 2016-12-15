/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"
#include "../util/log.h"
#include "../util/config.h"
#include "codec/decode.h"
#ifdef USE_LEVELDB
#include "leveldb/iterator.h"
#else
#include "rocksdb/iterator.h"
#define leveldb rocksdb
#endif

Iterator::Iterator(leveldb::Iterator *it,
		const std::string &end,
		uint64_t limit,
		Direction direction,
		const leveldb::Snapshot *snapshot)
{
	this->it = it;
	this->end = end;
	this->limit = limit;
	this->is_first = true;
	this->direction = direction;
	this->snapshot = snapshot;
}

Iterator::~Iterator(){
	delete it;
}

Bytes Iterator::key(){
	leveldb::Slice s = it->key();
	return Bytes(s.data(), s.size());
}

Bytes Iterator::val(){
	leveldb::Slice s = it->value();
	return Bytes(s.data(), s.size());
}

bool Iterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}

bool Iterator::next(){
	if(limit == 0){
		return false;
	}
	if(is_first){
		is_first = false;
	}else{
		if(direction == FORWARD){
			it->Next();
		}else{
			it->Prev();
		}
	}

	if(!it->Valid()){
		// make next() safe to be called after previous return false.
		limit = 0;
		return false;
	}
	if(direction == FORWARD){
		if(!end.empty() && it->key().compare(end) > 0){
			limit = 0;
			return false;
		}
	}else{
		if(!end.empty() && it->key().compare(end) < 0){
			limit = 0;
			return false;
		}
	}
	limit --;
	return true;
}


/* KV */

// todo r2m adaptation
KIterator::KIterator(Iterator *it){
	this->it = it;
	this->return_val_ = true;
}

KIterator::~KIterator(){
	delete it;
}

void KIterator::return_val(bool onoff){
	this->return_val_ = onoff;
}

bool KIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		Bytes vs = it->val();
		//dump(ks.data(), ks.size(), "z.next");
		//dump(vs.data(), vs.size(), "z.next");
		if(ks.data()[0] != DataType::KV){
			return false;
		}
		if(decode_kv_key(ks, &this->key) == -1){
			continue;
		}
		if(return_val_){
			this->val.assign(vs.data(), vs.size());
		}
		return true;
	}
	return  false;
}
/* KV */

// todo r2m adaptation
MIterator::MIterator(Iterator *it){
	this->it = it;
}

MIterator::~MIterator(){
	delete it;
}

bool MIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		if (ks.data()[0] != DataType::META) {
			return false;
		}

		MetaKey mk;
		if (mk.DecodeMetaKey(it->key().String()) == -1){
			continue;
		}
		key = mk.key;

		Bytes vs = it->val();
		this->dataType = vs.data()[0];
		return true;
	}
	return false;
}

/* HASH */

// todo r2m adaptation
HIterator::HIterator(Iterator *it, const Bytes &name, uint16_t version){
	this->it = it;
	this->name.assign(name.data(), name.size());
	this->version = version;
	this->return_val_ = true;
}

HIterator::~HIterator(){
	delete it;
}

void HIterator::return_val(bool onoff){
	this->return_val_ = onoff;
}

bool HIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		Bytes vs = it->val();
		//dump(ks.data(), ks.size(), "z.next");
		//dump(vs.data(), vs.size(), "z.next");
		HashItemKey hk;
		if(hk.DecodeItemKey(ks.String()) == -1){
			continue;
		}
		if(hk.key != this->name || hk.version != this->version){
			return false;
		}
		this->key = hk.field;
		if(return_val_){
			this->val.assign(vs.data(), vs.size());
		}
		return true;
	}
	return false;
}

/* SET */
SIterator::SIterator(Iterator *it, const Bytes &name, uint16_t version) {
	this->it = it;
	this->name.assign(name.data(), name.size());
	this->version = version;
}

SIterator::~SIterator() {
	delete it;
	it = NULL;
}

bool SIterator::next() {
	while (it->next()){
		Bytes ks = it->key();

		SetItemKey sk;
		if (sk.DecodeItemKey(ks.String()) == -1){
			continue;
		}
		if(sk.key != this->name || sk.version != this->version){
			return false;
		}
		this->key = sk.field;

		return true;
	}
	return false;
}

/* ZSET */

// todo r2m adaptation
ZIterator::ZIterator(Iterator *it, const Bytes &name, uint16_t version){
	this->it = it;
	this->name.assign(name.data(), name.size());

	this->version = version;
}

ZIterator::~ZIterator(){
	delete it;
}

bool ZIterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}

bool ZIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		//Bytes vs = it->val();
//		dump(ks.data(), ks.size(), "z.next");
//		dump(vs.data(), vs.size(), "z.next");

		ZScoreItemKey zk;
		if(zk.DecodeItemKey(ks.String()) == -1){
			continue;
		}
		if(zk.key != this->name || zk.version != this->version){
			return false;
		}

		this->key = zk.field;
		this->score = zk.score;

//		this->key = zk.field;
		return true;

	}
	return false;
}

LIterator::LIterator(Iterator *it, const Bytes &name, uint16_t version) {
	this->it = it;
	this->name.assign(name.data(), name.size());
	this->version = version;
}

LIterator::~LIterator() {
	delete it;
	it = NULL;
}

bool LIterator::next() {
	while (it->next()){
		Bytes ks = it->key();

		ListItemKey sk;
		if (sk.DecodeItemKey(ks.String()) == -1){
			continue;
		}
		if(sk.key != this->name || sk.version != this->version){
			return false;
		}
		this->seq = sk.seq;

		return true;
	}
	return false;
}



EIterator::EIterator(Iterator *it){
	this->it = it;
}

EIterator::~EIterator(){
	delete it;
}

bool EIterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}

bool EIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		//Bytes vs = it->val();
//		dump(ks.data(), ks.size(), "z.next");
//		dump(vs.data(), vs.size(), "z.next");

		if (ks.size() < 1 || ks.data()[0] != DataType::ESCORE) {
			return false;
		}

		EScoreItemKey ek;
		if(ek.DecodeItemKey(ks.String()) == -1){
			continue;
		}

		this->key = ek.field;
		this->score = ek.score;

//		this->key = zk.field;
		return true;

	}
	return false;
}