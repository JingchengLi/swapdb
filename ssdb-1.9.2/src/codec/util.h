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

#endif //SSDB_UTIL_H
