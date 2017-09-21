//
// Created by a1 on 16-11-2.
//

#ifndef SSDB_UTIL_H
#define SSDB_UTIL_H

#include <stdlib.h>
#include <stdint.h>
#include <string>

using namespace std;

#define KEY_DELETE_MASK 'D'
#define KEY_ENABLED_MASK 'E'


#define POS_TYPE 0
#define POS_DEL  3

#define ZSET_SCORE_SHIFT 1000000000000000000LL

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)     /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)     /* Set if key exists. */
#define OBJ_SET_EX (1<<2)     /* Set if time in seconds is given */
#define OBJ_SET_PX (1<<3)     /* Set if time in ms in given */


/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zset_one() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */


unsigned int keyHashSlot(const char *key, int keylen);



class DataType{
public:

    static const char META		= 'M'; // meta key
    static const char KV		= 'k'; // meta value string
    static const char HSIZE		= 'H'; // meta value hash
    static const char ZSIZE		= 'Z'; // meta value zset
    static const char SSIZE		= 'S'; // meta value set
    static const char LSIZE		= 'L'; // meta value list


    static const char ITEM		= 'S'; // meta value item

    static const char ZSCORE	= 'z';

    static const char ESCORE	= 'T'; // expire key
    static const char EKEY   	= 'E'; // expire timestamp key

    static const char DELETE	= KEY_DELETE_MASK;


    static const char REPOKEY		= 'L';
    static const char REPOITEM		= 'l';

};


/*
inline uint64_t encodeScore(const double score) {
    int64_t iscore;
    if (score < 0) {
        iscore = (int64_t)(score * 100000LL - 0.5) + ZSET_SCORE_SHIFT;
    } else {
        iscore = (int64_t)(score * 100000LL + 0.5) + ZSET_SCORE_SHIFT;
    }
    return (uint64_t)(iscore);
}


inline double decodeScore(const int64_t score) {
    return (double)(score - ZSET_SCORE_SHIFT) / 100000.0;
}
*/

static_assert(sizeof(uint64_t) == sizeof(double), "sizeof(uint64_t) != sizeof(double), please check platform");

static const unsigned int bit_num = sizeof(uint64_t) * 8;
static const uint64_t bit_one = 0x01;;


inline double decodeScore(uint64_t c) {

    uint64_t pos_num = c >> (bit_num - 1);
    if (pos_num == bit_one) {
        //score >= 0
        c = (c & ~(bit_one << (bit_num - 1))); //set 0
    } else {
        c = c ^ UINT64_MAX;

    }
    double score = *reinterpret_cast<double *>(&c);
    return score;
}

inline uint64_t encodeScore(double score) {

    uint64_t it = *reinterpret_cast<uint64_t *>(&score);
    uint64_t pos_num = it >> (bit_num - 1);
    if (pos_num == bit_one) {
        it = it ^ UINT64_MAX;
    } else {
        it = it | (bit_one << (bit_num - 1)); //set 1
    }

    return it;
}





#endif //SSDB_UTIL_H
