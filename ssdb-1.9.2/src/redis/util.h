//
// Created by zts on 17-3-3.
//

#ifndef REDIS_UTIL_H
#define REDIS_UTIL_H

#include "stdarg.h"

//#define rdbExitReportCorruptRDB(...) {exit(-1);}
#define rdbExitReportCorruptRDB(...) rdbCheckThenExit(__LINE__,__VA_ARGS__)


void rdbCheckThenExit(int linenum, const char *reason, ...) {
    va_list ap;
    char msg[1024];
    int len;

    len = snprintf(msg,sizeof(msg),
                   "Internal error in RDB reading function at rdb.c:%d -> ", linenum);
    va_start(ap,reason);
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
    va_end(ap);

    //reset part ignored

    exit(1);
}


#endif //REDIS_UTIL_H
