/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_BYTES_H_
#define UTIL_BYTES_H_

#include "strings.h"

// readonly
// to replace std::string
class Bytes{
	private:
		const char *data_;
		int size_;
	public:
		Bytes(){
			data_ = "";
			size_ = 0;
		}

		explicit Bytes(void *data, int size){
			data_ = (char *)data;
			size_ = size;
		}

		explicit Bytes(const char *data, int size){
			data_ = data;
			size_ = size;
		}

		Bytes(const std::string &str){
			data_ = str.data();
			size_ = (int)str.size();
		}

		Bytes(const char *str){
			data_ = str;
			size_ = (int)strlen(str);
		}

		Bytes(const Bytes &bytes){
			data_ = bytes.data_;
			size_ = bytes.size_;
		}

        //be careful~
		inline char operator[](unsigned int e) const
		{
 			return *(data_ + e);
		}

		const char* data() const{
			return data_;
		}

		bool empty() const{
			return size_ == 0;
		}

		int size() const{
			return size_;
		}

		void assign(const char *data, int size) {
			data_ = (char *)data;
			size_ = size;
		}

		int compare(const Bytes &b) const{
			const int min_len = (size_ < b.size_) ? size_ : b.size_;
			int r = memcmp(data_, b.data_, min_len);
			if(r == 0){
				if (size_ < b.size_) r = -1;
				else if (size_ > b.size_) r = +1;
			}
			return r;
		}

		std::string String() const{
			return std::string(data_, size_);
		}

		int Int() const{
			return str_to_int(data_, size_);
		}

		int64_t Int64() const{
			return str_to_int64(data_, size_);
		}

		uint64_t Uint64() const{
			return str_to_uint64(data_, size_);
		}

		double Double() const{
			return str_to_double(data_, size_);
		}

		long double LDouble() const{
			return str_to_long_double(data_, size_);
		}
};

inline
bool operator==(const Bytes &x, const Bytes &y){
	return ((x.size() == y.size()) &&
			(memcmp(x.data(), y.data(), x.size()) == 0));
}

inline
bool operator!=(const Bytes &x, const Bytes &y){
	return !(x == y);
}

inline
bool operator>(const Bytes &x, const Bytes &y){
	return x.compare(y) > 0;
}

inline
bool operator>=(const Bytes &x, const Bytes &y){
	return x.compare(y) >= 0;
}

inline
bool operator<(const Bytes &x, const Bytes &y){
	return x.compare(y) < 0;
}

inline
bool operator<=(const Bytes &x, const Bytes &y){
	return x.compare(y) <= 0;
}



class Buffer{
	private:
		char *buf;
		char *data_;
		int size_;
		int total_;
	public:
		Buffer(int total);
		~Buffer();

		// 缓冲区大小
		int total() const{
			return total_;
		}

		bool empty() const{
			return size_ == 0;
		}

		// 数据
		char* data() const{
			return data_;
		}

		// 数据大小
		int size() const{
			return size_;
		}

		// 指向空闲处
		char* slot() const{
			return data_ + size_;
		}

		int space() const{
			return total_ - (int)(data_ - buf) - size_;
		}

		void incr(int num){
			size_ += num;
		}

		void decr(int num){
			size_ -= num;
			data_ += num;
		}

		// 保证不改变后半段的数据, 以便使已生成的 Bytes 不失效.
		void nice();
		// 扩大缓冲区
		int grow();
		// 缩小缓冲区, 如果指定的 total 太小超过数据范围, 或者不合理, 则不会缩小
		void shrink(int total=0);

		std::string stats() const;
		int read_record(Bytes *s);

		int append(char c);
		int append(const char *p);
		int append(const void *p, int size);
		int append(const Bytes &s);

		int append_record(const Bytes &s);
};


class Decoder{
private:
	const char *data_;
	int size_;
	Decoder(){}
public:
	Decoder(const char *p, int s){
		this->data_ = p;
		this->size_ = s;
	}
	int skip(int n){
		if(size_ < n){
			return -1;
		}
		data_ += n;
		size_ -= n;
		return n;
	}
	const char *data() const {
		return data_;
	}
	int size() const {
		return size_;
	}
	int read_uint16(uint16_t *ret){
		if(size_t(size_) < sizeof(uint16_t)){
			return -1;
		}
		if(ret){
			*ret = *(uint16_t *)data_;
		}
		data_ += sizeof(uint16_t);
		size_ -= sizeof(uint16_t);
		return sizeof(uint16_t);
	}
	int read_int64(int64_t *ret){
		if(size_t(size_) < sizeof(int64_t)){
			return -1;
		}
		if(ret){
			*ret = *(int64_t *)data_;
		}
		data_ += sizeof(int64_t);
		size_ -= sizeof(int64_t);
		return sizeof(int64_t);
	}
	int read_uint64(uint64_t *ret){
		if(size_t(size_) < sizeof(uint64_t)){
			return -1;
		}
		if(ret){
			*ret = *(uint64_t *)data_;
		}
		data_ += sizeof(uint64_t);
		size_ -= sizeof(uint64_t);
		return sizeof(uint64_t);
	}
	int read_data(std::string *ret) {
		int n = size_;
		if (ret) {
			ret->assign(data_, size_);
		}
		data_ += size_;
		size_ = 0;
		return n;
	}
	int read_8_data(std::string *ret=NULL){
		if(size_ < 1){
			return -1;
		}
		int len = (uint8_t)data_[0];
		data_ += 1;
		size_ -= 1;
		if(size_ < len){
			return -1;
		}
		if(ret){
			ret->assign(data_, len);
		}
		data_ += len;
		size_ -= len;
		return 1 + len;
	}
	int read_16_data(std::string *ret=NULL){
		if(size_ < 2){
			return -1;
		}
		uint16_t len = *(uint16_t *)data_;
		len = be16toh(len);
		data_ += 2;
		size_ -= 2;
		if(size_ < len){
			return -1;
		}
		if(ret){
			ret->assign(data_, len);
		}
		data_ += len;
		size_ -= len;
		return 2 + len;
	}

	int read_data(Bytes &str){
		int n = size_;

        str.assign(data_, size_);

		data_ += size_;
		size_ = 0;
		return n;
	}
};

#endif

