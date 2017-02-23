/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_STRING_H
#define UTIL_STRING_H

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string>
#include <algorithm>
#include <limits.h>


/* Convert a long double into a string. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The function returns the length of the string or zero if there was not
 * enough buffer room to store it. */
static inline
int ld2string(char *buf, size_t len, long double value, int humanfriendly) {
	size_t l;

	if (std::isinf(value)) {
		/* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
		if (len < 5) return 0; /* No room. 5 is "-inf\0" */
		if (value > 0) {
			memcpy(buf,"inf",3);
			l = 3;
		} else {
			memcpy(buf,"-inf",4);
			l = 4;
		}
	} else if (humanfriendly) {
		/* We use 17 digits precision since with 128 bit floats that precision
         * after rounding is able to represent most small decimal numbers in a
         * way that is "non surprising" for the user (that is, most small
         * decimal numbers will be represented in a way that when converted
         * back into a string are exactly the same as what the user typed.) */
		l = snprintf(buf,len,"%.17Lf", value);
		if (l+1 > len) return 0; /* No room. */
		/* Now remove trailing zeroes after the '.' */
		if (strchr(buf,'.') != NULL) {
			char *p = buf+l-1;
			while(*p == '0') {
				p--;
				l--;
			}
			if (*p == '.') l--;
		}
	} else {
		l = snprintf(buf,len,"%.17Lg", value);
		if (l+1 > len) return 0; /* No room. */
	}
	buf[l] = '\0';
	return l;
}

inline static
int is_empty_str(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return *p == '\0';
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *ltrim(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return (char *)p;
}

/* 返回指向字符串结尾的指针, 会修改字符串内容 */
inline static
char *rtrim(char *str){
	char *p;
	p = str + strlen(str) - 1;
	while(p >= str && isspace(*p)){
		p--;
	}
	*(++p) = '\0';
	return p;
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *trim(char *str){
	char *p;
	p = ltrim(str);
	rtrim(p);
	return p;
}

inline static
std::string strtrim(const std::string& str) {
	std::string::size_type pos = str.find_first_not_of(' ');
	if (pos == std::string::npos)
	{
		return str;
	}
	std::string::size_type pos2 = str.find_last_not_of(' ');
	if (pos2 != std::string::npos)
	{
		return str.substr(pos, pos2 - pos + 1);
	}
	return str.substr(pos);
}

inline static
void strtolower(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

inline static
void strtoupper(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::toupper);
}

inline static
std::string real_dirname(const char *filepath){
	std::string dir;
	if(filepath[0] != '/'){
		char buf[1024];
		char *p = getcwd(buf, sizeof(buf));
		if(p != NULL){
			dir.append(p);
		}
		dir.append("/");
	}

	const char *p = strrchr(filepath, '/');
	if(p != NULL){
		dir.append(filepath, p - filepath);
	}
	return dir;
}

inline static
std::string str_escape(const char *s, int size){
	static const char *hex = "0123456789abcdef";
	std::string ret;
	for(int i=0; i<size; i++){
		char c = s[i];
		switch(c){
			case '\r':
				ret.append("\\r");
				break;
			case '\n':
				ret.append("\\n");
				break;
			case '\t':
				ret.append("\\t");
				break;
			case '\\':
				ret.append("\\\\");
				break;
			case ' ':
				ret.push_back(c);
				break;
			default:
				if(c >= '!' && c <= '~'){
					ret.push_back(c);
				}else{
					ret.append("\\x");
					unsigned char d = c;
					ret.push_back(hex[d >> 4]);
					ret.push_back(hex[d & 0x0f]);
				}
				break;
		}
	}
	return ret;
}

inline static
std::string str_escape(const std::string &s){
	return str_escape(s.data(), (int)s.size());
}

inline static
int hex_int(char c){
	if(c >= '0' && c <= '9'){
		return c - '0';
	}else{
		return c - 'a' + 10;
	}
}

inline static
std::string str_unescape(const char *s, int size){
	std::string ret;
	for(int i=0; i<size; i++){
		char c = s[i];
		if(c != '\\'){
			ret.push_back(c);
		}else{
			if(i >= size - 1){
				continue;
			}
			char c2 = s[++i];
			switch(c2){
				case 'a':
					ret.push_back('\a');
					break;
				case 'b':
					ret.push_back('\b');
					break;
				case 'f':
					ret.push_back('\f');
					break;
				case 'v':
					ret.push_back('\v');
					break;
				case 'r':
					ret.push_back('\r');
					break;
				case 'n':
					ret.push_back('\n');
					break;
				case 't':
					ret.push_back('\t');
					break;
				case '\\':
					ret.push_back('\\');
					break;
				case 'x':
					if(i < size - 2){
						char c3 = s[++i];
						char c4 = s[++i];
						ret.push_back((char)((hex_int(c3) << 4) + hex_int(c4)));
					}
					break;
				default:
					ret.push_back(c2);
					break;
			}
		}
	}
	return ret;
}

inline static
std::string str_unescape(const std::string &s){
	return str_unescape(s.data(), (int)s.size());
}

inline static
std::string hexmem(const void *p, int size){
	return str_escape((char *)p, size);
	/*
	std::string ret;
	char buf[4];
	for(int i=0; i<size; i++){
		char c = ((char *)p)[i];
		if(isalnum(c) || isprint(c)){
			ret.append(1, c);
		}else{
			switch(c){
				case '\r':
					ret.append("\\r", 2);
					break;
				case '\n':
					ret.append("\\n", 2);
					break;
				default:
					sprintf(buf, "\\%02x", (unsigned char)c);
					ret.append(buf, 3);
			}
		}
	}
	return ret;
	*/
}


template <typename T>
inline static
std::string hexstr(const T &t){
	return hexmem(t.data(), t.size());
}

// TODO: mem_printf("%5c%d%s", p, size);
static inline
void dump(const void *p, int size, const char *msg = NULL){
	if(msg == NULL){
		printf("dump <");
	}else{
		printf("%s <", msg);
	}
	std::string s = hexmem(p, size);
	printf("%s>\n", s.c_str());
}


static inline
std::string str(const char *s){
	return std::string(s);
}

static inline
std::string str(int v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%d", v);
	return std::string(buf);
}

static inline
std::string str(int64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRId64 "", v);
	return std::string(buf);
}

static inline
std::string str(uint64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
	return std::string(buf);
}

static inline
std::string str(long double v){
	char buf[256];
	int len = ld2string(buf,sizeof(buf),v,1);

	return std::string(buf, len);
}

static inline
std::string str(double v){
//	char buf[21] = {0};
//	if(v - floor(v) == 0){
//		snprintf(buf, sizeof(buf), "%.0f", v);
//	}else{
//		snprintf(buf, sizeof(buf), "%f", v);
//	}
//	return std::string(buf);
//
	char buf[256];
	int len = ld2string(buf,sizeof(buf),v,1);

	return std::string(buf, len);
//	new = sdsnewlen(buf,len);

}

static inline
std::string str(float v){
	return str((double)v);
}


// all str_to_xx methods set errno on error

static inline
int str_to_int(const std::string &str){
	const char *start = str.c_str();
	char *end;
	int ret = (int)strtol(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int str_to_int(const char *p, int size){
	return str_to_int(std::string(p, size));
}

static inline
int64_t str_to_int64(const std::string &str){
	const char *start = str.c_str();
	char *end;
	int64_t ret = (int64_t)strtoll(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int64_t str_to_int64(const char *p, int size){
	return str_to_int64(std::string(p, size));
}

static inline
uint64_t str_to_uint64(const std::string &str){
	const char *start = str.c_str();
	char *end;
	uint64_t ret = (uint64_t)strtoull(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
uint64_t str_to_uint64(const char *p, int size){
	return str_to_uint64(std::string(p, size));
}

static inline
long double str_to_long_double(const char *s, int slen){
	char buf[256];
	long double value;
	char *eptr;

	if (slen >= sizeof(buf)) return 0;
	memcpy(buf,s,slen);
	buf[slen] = '\0';

	errno = 0;
	value = std::strtold(buf, &eptr);
	if (isspace(buf[0]) || eptr[0] != '\0' ||
		(errno == ERANGE &&
		 (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
		errno == EINVAL ||
		std::isnan(value)) {
		errno = EINVAL; //we set all err  EINVAL here
		return 0L;
	}

	return value;
}

static inline
double str_to_double(const char *s, int slen){
	char buf[256];
	double value;
	char *eptr;

	if (slen >= sizeof(buf)) return 0;
	memcpy(buf,s,slen);
	buf[slen] = '\0';

	errno = 0;
	value = strtod(buf, &eptr);
	if (isspace(buf[0]) || eptr[0] != '\0' ||
		(errno == ERANGE &&
		 (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
		errno == EINVAL ||
		std::isnan(value)) {
		errno = EINVAL; //we set all err  EINVAL here
		return 0;
	}

	return value;

//	std::string str(p, size);
//	const char *start = str.c_str();
//	char *end;
//	double ret = (double)strtod(start, &end);
//	// the WHOLE string must be string represented integer
//	if((str.size()> 0 && isspace(str[0])) || *end == '\0' && size_t(end - start) == str.size()){
//		errno = 0;
//	}else{
//		// strtoxx do not set errno all the time!
//		if(errno == 0){
//			errno = EINVAL;
//		}
//	}
//	return ret;

//	return atof(std::string(p, size).c_str());
}

static inline
std::string substr(const std::string &str, int start, int size){
	if(start < 0){
		start = (int)str.size() + start;
	}
	if(size < 0){
		// 忽略掉 abs(size) 个字节
		size = ((int)str.size() + size) - start;
	}
	if(start < 0 || size_t(start) >= str.size() || size < 0){
		return "";
	}
	return str.substr(start, size);
}

static inline
std::string str_slice(const std::string &str, int start, int end){
	if(start < 0){
		start = (int)str.size() + start;
	}
	int size;
	if(end < 0){
		size = ((int)str.size() + end + 1) - start;
	}else{
		size = end - start + 1;
	}
	if(start < 0 || size_t(start) >= str.size() || size < 0){
		return "";
	}
	return str.substr(start, size);
}

static inline
int bitcount(const char *p, int size){
	int n = 0;
	for(int i=0; i<size; i++){
		unsigned char c = (unsigned char)p[i];
		while(c){
			n += c & 1;
			c = c >> 1;
		}
	}
	return n;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
static inline
uint32_t digits10(uint64_t v) {
	if (v < 10) return 1;
	if (v < 100) return 2;
	if (v < 1000) return 3;
	if (v < 1000000000000UL) {
		if (v < 100000000UL) {
			if (v < 1000000) {
				if (v < 10000) return 4;
				return 5 + (v >= 100000);
			}
			return 7 + (v >= 10000000UL);
		}
		if (v < 10000000000UL) {
			return 9 + (v >= 1000000000UL);
		}
		return 11 + (v >= 100000000000UL);
	}
	return 12 + digits10(v / 1000000000000UL);
}

/* Like digits10() but for signed values. */
static inline
uint32_t sdigits10(int64_t v) {
	if (v < 0) {
		/* Abs value of LLONG_MIN requires special handling. */
		uint64_t uv = (v != LLONG_MIN) ?
					  (uint64_t)-v : ((uint64_t) LLONG_MAX)+1;
		return digits10(uv)+1; /* +1 for the minus. */
	} else {
		return digits10(v);
	}
}

/* Convert a string into a double. Returns 1 if the string could be parsed
 * into a (non-overflowing) double, 0 otherwise. The value will be set to
 * the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a double: no spaces or other characters before or after the string
 * representing the number are accepted. */
static inline
int string2ld(const char *s, size_t slen, long double *dp) {
	char buf[256];
	long double value;
	char *eptr;

	if (slen >= sizeof(buf)) return 0;
	memcpy(buf,s,slen);
	buf[slen] = '\0';

	errno = 0;
	value = strtold(buf, &eptr);
	if (isspace(buf[0]) || eptr[0] != '\0' ||
		(errno == ERANGE &&
		 (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
		errno == EINVAL ||
		std::isnan(value))
		return 0;

	if (dp) *dp = value;
	return 1;
}


/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
static inline
int ll2string(char *dst, size_t dstlen, long long svalue) {
	static const char digits[201] =
			"0001020304050607080910111213141516171819"
					"2021222324252627282930313233343536373839"
					"4041424344454647484950515253545556575859"
					"6061626364656667686970717273747576777879"
					"8081828384858687888990919293949596979899";
	int negative;
	unsigned long long value;

	/* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
	if (svalue < 0) {
		if (svalue != LLONG_MIN) {
			value = -svalue;
		} else {
			value = ((unsigned long long) LLONG_MAX)+1;
		}
		negative = 1;
	} else {
		value = svalue;
		negative = 0;
	}

	/* Check length. */
	uint32_t const length = digits10(value)+negative;
	if (length >= dstlen) return 0;

	/* Null term. */
	uint32_t next = length;
	dst[next] = '\0';
	next--;
	while (value >= 100) {
		int const i = (value % 100) * 2;
		value /= 100;
		dst[next] = digits[i + 1];
		dst[next - 1] = digits[i];
		next -= 2;
	}

	/* Handle last 1-2 digits. */
	if (value < 10) {
		dst[next] = '0' + (uint32_t) value;
	} else {
		int i = (uint32_t) value * 2;
		dst[next] = digits[i + 1];
		dst[next - 1] = digits[i];
	}

	/* Add sign. */
	if (negative) dst[0] = '-';
	return length;
}

static inline
int string2ll(const char *s, size_t slen, long long *value) {
	const char *p = s;
	size_t plen = 0;
	int negative = 0;
	unsigned long long v;

	if (plen == slen)
		return 0;

	/* Special case: first and only digit is 0. */
	if (slen == 1 && p[0] == '0') {
		if (value != NULL) *value = 0;
		return 1;
	}

	if (p[0] == '-') {
		negative = 1;
		p++; plen++;

		/* Abort on only a negative sign. */
		if (plen == slen)
			return 0;
	}

	/* First digit should be 1-9, otherwise the string should just be 0. */
	if (p[0] >= '1' && p[0] <= '9') {
		v = p[0]-'0';
		p++; plen++;
	} else if (p[0] == '0' && slen == 1) {
		*value = 0;
		return 1;
	} else {
		return 0;
	}

	while (plen < slen && p[0] >= '0' && p[0] <= '9') {
		if (v > (ULLONG_MAX / 10)) /* Overflow. */
			return 0;
		v *= 10;

		if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
			return 0;
		v += p[0]-'0';

		p++; plen++;
	}

	/* Return if not all bytes were used. */
	if (plen < slen)
		return 0;

	if (negative) {
		if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
			return 0;
		if (value != NULL) *value = -v;
	} else {
		if (v > LLONG_MAX) /* Overflow. */
			return 0;
		if (value != NULL) *value = v;
	}
	return 1;
}

// is big endia. TODO: auto detect
#if 0
	#define big_endian(v) (v)
#else
	static inline
	uint16_t big_endian(uint16_t v){
		return (v>>8) | (v<<8);
	}

	static inline
	uint32_t big_endian(uint32_t v){
		return (v >> 24) | ((v >> 8) & 0xff00) | ((v << 8) & 0xff0000) | (v << 24);
	}

	static inline
	uint64_t big_endian(uint64_t v){
		uint32_t h = v >> 32;
		uint32_t l = v & 0xffffffffull;
		return big_endian(h) | ((uint64_t)big_endian(l) << 32);
	}
#endif


#endif
