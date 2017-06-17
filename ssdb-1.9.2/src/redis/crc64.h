#ifndef CRC64_H
#define CRC64_H

#include "redis/crc/crc64speed.h"

/* Run the init() function exactly once.  If pthread.h is not included, then
   this macro will use a simple static state variable for the purpose, which is
   not thread-safe.  The init function must be of the type void init(void). */
#ifdef PTHREAD_ONCE_INIT
#  define ONCE(init) \
    do { \
        static pthread_once_t once = PTHREAD_ONCE_INIT; \
        pthread_once(&once, init); \
    } while (0)
#else
#  define ONCE(init) \
    do { \
        static volatile int once = 1; \
        if (once) { \
            if (once++ == 1) { \
                init(); \
                once = 0; \
            } \
            else \
                while (once) \
                    ; \
        } \
    } while (0)
#endif

inline void init_crc64speed_table(){
    crc64speed_init_native();
}

inline uint64_t crc64_fast(uint64_t crc, const void *s, const uint64_t l) {
    ONCE(init_crc64speed_table);
    return crc64speed_native(crc, s, l);
}


#endif
