//
// Created by zts on 17-2-17.
//

#ifndef SSDB_INTERNAL_ERROR_H
#define SSDB_INTERNAL_ERROR_H

#include "map"
#include "string"

#define SUCCESS           0
#define ERR               -1
#define STORAGE_ERR       -2
#define WRONG_TYPE_ERR    -3
#define MKEY_DECODEC_ERR  -4
#define MKEY_RESIZE_ERR   -5
#define INT_OVERFLOW      -6
#define DBL_OVERFLOW      -7
#define INVALID_INT      -8
#define INVALID_DBL      -9
#define INVALID_INCR      -10
#define STRING_OVERMAX      -11

#endif //SSDB_INTERNAL_ERROR_H
