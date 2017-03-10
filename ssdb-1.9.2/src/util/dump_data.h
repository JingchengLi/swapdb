//
// Created by zts on 17-1-10.
//

#ifndef SSDB_BLOCKING_QUEUE_H
#define SSDB_BLOCKING_QUEUE_H

#include <mutex>
#include <condition_variable>
#include <deque>
#include <string>
#include <sstream>
#include "strings.h"


struct DumpData {
    std::string key;
    std::string data;

    int64_t expire;
    bool replace;
    bool noreply;

    DumpData(const std::string &key, const std::string &data, int64_t expire, bool replace, bool noreply) : key(key), data(data),
                                                                                              expire(expire),
                                                                                              replace(replace),
                                                                                              noreply(noreply) {}
};

#endif //SSDB_BLOCKING_QUEUE_H
