//
// Created by zts on 17-3-3.
//

#include <util/strings.h>
#include "t_cursor.h"

uint64_t RedisCursorService::GetNewRedisCursor(const std::string &element) {
    Locking l(&mutex);

    RedisCursor cursor;
    cursor.element = element;
    cursor.ts = time(NULL);
    if (m_redis_cursor_table.empty())
    {
        cursor.cursor = 1;
    }
    else
    {
        cursor.cursor = (m_redis_cursor_table.rbegin()->second.cursor + 1);
    }
    m_redis_cursor_table.insert(RedisCursorTable::value_type(cursor.cursor, cursor));
    return cursor.cursor;
}

int RedisCursorService::FindElementByRedisCursor(const std::string &cursor, std::string &element) {
    uint64_t cursor_int = str_to_uint64(cursor.data(), cursor.size());

    if (errno == EINVAL) {
        return -1;
    }

    Locking l(&mutex);
    RedisCursorTable::iterator it = m_redis_cursor_table.find(cursor_int);
    if (it == m_redis_cursor_table.end())
    {
        return -1;
    }
    element = it->second.element;
    return 0;
}

void RedisCursorService::ClearExpireRedisCursor() {
    Locking l(&mutex);


    while (!m_redis_cursor_table.empty())
    {
        RedisCursorTable::iterator it = m_redis_cursor_table.begin();
        if (time(NULL) - it->second.ts >= scan_cursor_expire_after)
        {
            m_redis_cursor_table.erase(it);
        }
        else
        {
            return;
        }
    }
}
