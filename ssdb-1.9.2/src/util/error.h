//
// Created by zts on 17-2-17.
//

#ifndef SSDB_REDIS_ERROR_H
#define SSDB_REDIS_ERROR_H

const int SUCCESS                      =0;
//const int ERR                         =   -1;
const int STORAGE_ERR                  =  -2;
const int WRONG_TYPE_ERR               =  -3;
const int MKEY_DECODEC_ERR             =  -4;
const int MKEY_RESIZE_ERR              =  -5;
const int INT_OVERFLOW                 =  -6;
const int DBL_OVERFLOW                 =  -7;
const int INVALID_INT                  =  -8;
const int INVALID_DBL                  =  -9;
const int INVALID_INCR_LEN             = -10;
const int STRING_OVERMAX               = -11;
const int INDEX_OUT_OF_RANGE           = -12;
const int INVALID_METAVAL              = -13;
const int SYNTAX_ERR                   = -14;
const int INVALID_EX_TIME              = -15;
const int INVALID_INCR_PDC_NAN_OR_INF  = -16;
const int NAN_SCORE                    = -17;
const int ZSET_INVALID_STR             = -19;
const int BUSY_KEY_EXISTS              = -20;
const int INVALID_DUMP_STR             = -21;
const int INVALID_ARGS                 = -22;
const int VALUE_OUT_OF_RANGE           = -23;

#endif //SSDB_REDIS_ERROR_H
