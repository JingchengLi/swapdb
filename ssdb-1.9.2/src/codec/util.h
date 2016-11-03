//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_UTIL_H
#define SSDB_UTIL_H

#include <stdlib.h>
#include <stdint.h>

#define KEY_DELETE_MASK 'D'
#define KEY_ENABLED_MASK 'E'

#define ZSET_SCORE_SHIFT 1000000000000000000LL

/* for linux x86-64/amd64/i386, it's little endian, don't need convert. */
#define memrev16ifbe(p)
#define memrev32ifbe(p)
#define memrev64ifbe(p)
#define intrev16ifbe(v) (v)
#define intrev32ifbe(v) (v)
#define intrev64ifbe(v) (v)

/* Different encoding/length possibilities */
#define ZIP_STR_MASK 0xc0
#define ZIP_INT_MASK 0x30
#define ZIP_STR_06B (0 << 6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (2 << 6)
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe
/* 4 bit integer immediate encoding */
#define ZIP_INT_IMM_MASK 0x0f
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */
#define ZIP_INT_IMM_VAL(v) (v & ZIP_INT_IMM_MASK)

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

unsigned int keyHashSlot(const char *key, int keylen);

void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding);
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding);

#endif //SSDB_UTIL_H
