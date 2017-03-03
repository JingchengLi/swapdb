//
// Created by zts on 17-3-3.
//

#ifndef SSDB_T_CURSOR_H
#define SSDB_T_CURSOR_H

#include <util/thread.h>
#include <map>

struct RedisCursor
{
    std::string element;
    time_t ts;
    uint64_t cursor;
    RedisCursor() :
            ts(0), cursor(0)
    {
    }
};
typedef std::map<uint64_t, RedisCursor> RedisCursorTable;

class RedisCursorService {
public:

    uint64_t GetNewRedisCursor(const std::string& element);

    int FindElementByRedisCursor(const std::string& cursor, std::string& element);

    void ClearExpireRedisCursor();

private:
    const int scan_cursor_expire_after = 60 * 1000;

    RedisCursorTable m_redis_cursor_table;

    Mutex mutex;

};




#endif //SSDB_T_CURSOR_H
