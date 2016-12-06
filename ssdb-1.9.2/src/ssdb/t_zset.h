/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ZSET_H_
#define SSDB_ZSET_H_

#include "ssdb_impl.h"

static inline
std::string encode_zsize_key(const Bytes &name){
	std::string buf;
	buf.append(1, DataType::ZSIZE);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_zsize_key(const Bytes &slice, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}

#endif
