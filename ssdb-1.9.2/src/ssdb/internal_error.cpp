//
// Created by zts on 17-2-18.
//


#include "../util/error.h"



std::map<int, std::string> SSDBErrMap = {
        {SUCCESS,          "success"},
        {ERR,              "ERR              "},
        {STORAGE_ERR,      "ERR STORAGE_ERR      "},
        {WRONG_TYPE_ERR,   "WRONGTYPE Operation against a key holding the wrong kind of value"},
        {MKEY_DECODEC_ERR, "ERR MKEY_DECODEC_ERR "},
        {MKEY_RESIZE_ERR,  "ERR MKEY_RESIZE_ERR  "},
        {INT_OVERFLOW,     "ERR increment or decrement would overflow"},
        {DBL_OVERFLOW,     "ERR increment or decrement would overflow"},
        {INVALID_INT,      "ERR value is not an integer"},
        {INVALID_DBL,      "ERR value is not a valid float"},
        {INVALID_INCR,     "ERR INVALID_INCR     "},
        {STRING_OVERMAX,   "ERR string exceeds maximum allowed size (512MB)"},

};


std::string GetErrorInfo(int ret) {

    auto pos = SSDBErrMap.find(ret);
    if (pos == SSDBErrMap.end()) {
        return "error";

    } else {
        return pos->second;
    }

}

