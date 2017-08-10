/*
Copyright (c) 2004-2017, JD.com Inc. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#include <sstream>
#include "internal_error.h"


std::map<int, std::string> SSDBErrMap = {
        {SUCCESS,          "success"},
//        {ERR,              "ERR              "},
        {STORAGE_ERR,      "ERR STORAGE_ERR      "},
        {WRONG_TYPE_ERR,   "WRONGTYPE Operation against a key holding the wrong kind of value"},
        {MKEY_DECODEC_ERR, "ERR MKEY_DECODEC_ERR "},
        {MKEY_RESIZE_ERR,  "ERR MKEY_RESIZE_ERR  "},
        {INVALID_METAVAL,  "ERR invalid MetaVal"},
        {INT_OVERFLOW,     "ERR increment or decrement would overflow"},
        {DBL_OVERFLOW,     "ERR increment or decrement would overflow"},
        {INVALID_INT,      "ERR value is not an integer or out of range"},
        {INVALID_DBL,      "ERR value is not a valid float"},
        {INVALID_INCR_LEN,     "ERR INVALID_INCR     "},
        {STRING_OVERMAX,   "ERR string exceeds maximum allowed size (512MB)"},
        {INDEX_OUT_OF_RANGE,"ERR index out of range"},
        {VALUE_OUT_OF_RANGE,"ERR value is out of range"},
        {SYNTAX_ERR,       "ERR syntax error"},
        {INVALID_EX_TIME,  "ERR invalid expire time"},
        {INVALID_INCR_PDC_NAN_OR_INF,  "ERR increment would produce NaN or Infinity"},
        {NAN_SCORE,        "ERR resulting score is not a float number (NaN)"},
//        {ZSET_OVERFLOW,    "ERR value is not a valid float or is less than ZSET_SCORE_MIN or greater than ZSET_SCORE_MAX"},
        {ZSET_INVALID_STR, "ERR min or max not valid string range item"},
        {BUSY_KEY_EXISTS,  "BUSYKEY Target key name already exists."},
        {INVALID_DUMP_STR,  "ERR DUMP payload version or checksum are wrong"},
        {INVALID_ARGS,  "ERR wrong number of arguments"},
};


std::string GetErrorInfo(int ret) {

    auto pos = SSDBErrMap.find(ret);
    if (pos == SSDBErrMap.end()) {
        std::stringstream convert;
        convert << ret;
        return "ERR server error with error code : " +  convert.str();
    } else {
        return pos->second;
    }

}

