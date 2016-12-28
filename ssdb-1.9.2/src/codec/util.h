//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_UTIL_H
#define SSDB_UTIL_H

#include "ssdb/const.h"

#include <stdlib.h>
#include <stdint.h>
#include <string>
using namespace std;

#define KEY_DELETE_MASK 'D'
#define KEY_ENABLED_MASK 'E'

#define ZSET_SCORE_SHIFT 1000000000000000000LL

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)     /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)     /* Set if key exists. */
#define OBJ_SET_EX (1<<2)     /* Set if time in seconds is given */
#define OBJ_SET_PX (1<<3)     /* Set if time in ms in given */

unsigned int keyHashSlot(const char *key, int keylen);

#endif //SSDB_UTIL_H
