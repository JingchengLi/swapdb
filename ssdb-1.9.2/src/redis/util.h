//
// Created by zts on 16-12-26.
//

#ifndef REDIS_UTIL_H
#define REDIS_UTIL_H


//TODO rdbExitReportCorruptRDB
#define rdbExitReportCorruptRDB(...) {exit(-1);}


int stringmatchlen(const char *p, int plen, const char *s, int slen, int nocase);



#endif //REDIS_UTIL_H
