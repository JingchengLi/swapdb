//
// Created by zts on 17-2-18.
//


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
        {INVALID_DBL,      "ERR value is not a valid float or out of range"},
        {INVALID_INCR,     "ERR INVALID_INCR     "},
        {STRING_OVERMAX,   "ERR string exceeds maximum allowed size (512MB)"},
        {INDEX_OUT_OF_RANGE,"ERR index out of range"},
        {SYNTAX_ERR,       "ERR syntax error"},
        {INVALID_EX_TIME,  "ERR invalid expire time"},
        {INVALID_INCR_PDC_NAN_OR_INF,  "ERR increment would produce NaN or Infinity"},

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

