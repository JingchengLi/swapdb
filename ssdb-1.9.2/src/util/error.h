//
// Created by zts on 17-2-17.
//

#ifndef SSDB_REDIS_ERROR_H
#define SSDB_REDIS_ERROR_H

#define SUCCESS           0
//#define ERR               -1
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
#define INDEX_OUT_OF_RANGE      -12
#define INVALID_METAVAL      -13
#define SYNTAX_ERR      -14
#define INVALID_EX_TIME  -15

#endif //SSDB_REDIS_ERROR_H
