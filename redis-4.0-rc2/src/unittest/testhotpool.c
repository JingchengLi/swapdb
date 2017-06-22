#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "sds.h"

#define serverAssert assert

#define zmalloc malloc
#define zfree free

#define HOT_POOL_TYPE 1
#define COLD_POOL_TYPE 2

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

const char* keys[] = {
    "xxx","yyy","zzz","rrr","sss","ttt",
    "uuu","vvv","www", "000","aaa","bbb","ccc","ddd","eee","fff","ggg","hhh",
    //"iii","jjj","kkk","lll", "mmm","nnn","ooo","ppp","qqq"
};

struct evictionPoolEntry *TestHotKeyPool ;
void newHotKeyPool(void) {
    struct evictionPoolEntry *ep;
    int j;
    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    TestHotKeyPool = ep;
}

void freeHotKeyPool(void) {
    int j;
    struct evictionPoolEntry *ep = TestHotKeyPool;
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        if (ep[j].key != NULL && ep[j].key != ep[j].cached) {
            sdsfree(ep[j].key);
        }
        ep[j].key = NULL;
        ep[j].dbid = 0;
    }
    serverAssert(TestHotKeyPool[0].key == NULL && TestHotKeyPool[0].idle == 0);
}

void printfHotKeyPool() {
    int j;
    struct evictionPoolEntry *ep = TestHotKeyPool;
    printf("========pool result===========\n");
    for (j = 0; j < EVPOOL_SIZE; j++) {
        printf("[%d]------key:%s,idle:%llu\n", j, ep[j].key, ep[j].idle);
    }
    printf("\n");
}

void replaceKeyInPool(struct evictionPoolEntry *pool, sds key, int dbid, unsigned long long idle, int pool_type) {
    /* Insert the element inside the pool.
    * First, find the first empty bucket or the first populated
    * bucket that has an idle time smaller than our idle time. */
    int k = 0, i = 0, old_index = -1;
    while (i < EVPOOL_SIZE && pool[i].key) {
        if (0 == sdscmp(key, pool[i].key)) {
            if (old_index != -1) printfHotKeyPool();
            /* the key should be unique in this pool before adding it. */
            serverAssert(old_index == -1);
            /* the key already exists in the pool. */
            old_index = i;
        }
        if ((pool_type == COLD_POOL_TYPE && pool[i].idle < idle) ||
            (pool_type == HOT_POOL_TYPE && pool[i].idle > idle))
            k++;
        i++;
    }

    if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
        /* Can't insert if the element is < the worst element we have
         * and there are no empty buckets. */
        return;
    } else {
        if (old_index != -1) {
            /* if the key is already in the pool, we just update the idle time, and
             * move the keys between the 'old_index' item and the 'k' item in the pool
             * to keep the order(by idle time of keys). */
            if (old_index == k) {
                /* update idle time */
                pool[old_index].idle = idle;
            } else if (old_index < k) {
                if (old_index+1 == k) {
                    k--;
                } else {
                    sds save = pool[old_index].key;
                    sds cached = pool[old_index].cached;

                    k--;
                    /* move keys forwards*/
                    memmove(pool+old_index, pool+old_index+1, (k-old_index)*sizeof(pool[0]));
                    /* re-use the buffer */
                    pool[k].cached = cached;
                    pool[k].key = save;
                }
            } else if (old_index > k) {
                sds save = pool[old_index].key;
                sds cached = pool[old_index].cached;
                /* move keys backwards*/
                memmove(pool+k+1, pool+k, (old_index-k)*sizeof(pool[0]));
                /* re-use the buffer */
                pool[k].cached = cached;
                pool[k].key = save;
            }
            /* update idle time */
            pool[k].idle = idle;

            printf("key: %s is already in %s pool, update its idle value\n",
                      key, pool_type == HOT_POOL_TYPE ? "hot" : "cold");
        } else {
            if (k < EVPOOL_SIZE && pool[k].key == NULL) {
                /* Inserting into empty position. No setup needed before insert. */
            } else {
                /* Inserting in the middle. Now k points to the first element
                 * greater than the element to insert.  */
                if (pool[EVPOOL_SIZE-1].key == NULL) {
                    /* Free space on the right? Insert at k shifting
                     * all the elements from k to end to the right. */

                    /* Save SDS before overwriting. */
                    sds cached = pool[EVPOOL_SIZE-1].cached;
                    memmove(pool+k+1,pool+k,
                            sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                    pool[k].cached = cached;
                } else {
                    /* No free space on right? Insert at k-1 */
                    k--;
                    /* Shift all elements on the left of k (included) to the
                     * left, so we discard the element with smaller idle time. */
                    sds cached = pool[0].cached; /* Save SDS before overwriting. */
                    if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                    memmove(pool,pool+1,sizeof(pool[0])*k);
                    pool[k].cached = cached;
                }
            }

            printf("key: %s is insert into %s pool\n", key, pool_type == HOT_POOL_TYPE ? "hot" : "cold");

            /* Try to reuse the cached SDS string allocated in the pool entry,
             * because allocating and deallocating this object is costly
             * (according to the profiler, not my fantasy. Remember:
             * premature optimizbla bla bla bla. */
            int klen = sdslen(key);
            if (klen > EVPOOL_CACHED_SDS_SIZE) {
                pool[k].key = sdsdup(key);
            } else {
                memcpy(pool[k].cached,key,klen+1);
                sdssetlen(pool[k].cached,klen);
                pool[k].key = pool[k].cached;
            }
            pool[k].idle = idle;
            pool[k].dbid = dbid;
        }
    }
}

void replaceKeyInHotPool(sds key, int dbid, unsigned long long idle) {
    replaceKeyInPool(TestHotKeyPool, key, dbid, idle, HOT_POOL_TYPE);
}



void test_hotpool_equal(int* id, struct evictionPoolEntry ep[], int size) {
    int i;
    int failed = 0;
    for (i = 0; i<size; i++) {
        if ((!ep[i].key && !TestHotKeyPool[i].key) ||
            (ep[i].key && TestHotKeyPool[i].key && 0 == strcmp(ep[i].key, TestHotKeyPool[i].key)
             && ep[i].idle == TestHotKeyPool[i].idle)) {
            printf("[  ok  ]TEST[%d][item %d] expect (key:%s,idle:%lld), result (key:%s, idle:%lld)\n",
                      *id, i, ep[i].key, ep[i].idle, TestHotKeyPool[i].key, TestHotKeyPool[i].idle);
        } else {
            printf("[failed]TEST[%d][item %d] expect (key:%s,idle:%lld), result (key:%s, idle:%lld)\n",
                      *id, i, ep[i].key, ep[i].idle, TestHotKeyPool[i].key, TestHotKeyPool[i].idle);
            failed = 1;
        }
    }
    if (failed)
        printf("TEST[%d] FAILED!\n", *id);
    else
        printf("TEST[%d] PASS\n", *id);
    (*id)++;
}

void removeHotKeys() {
    int k;
    int i = 0;
    struct evictionPoolEntry *pool = TestHotKeyPool;

    /* Go backward from best to worst element to evict. */
    for (k = EVPOOL_SIZE-1; k >= 0; k--) {
        if (pool[k].key == NULL) continue;
        if (i == 5) break;

        if (random()%10 == 0) continue;

        /* Remove the entry from the pool. */
        if (pool[k].key != pool[k].cached)
            sdsfree(pool[k].key);
        pool[k].key = NULL;
        pool[k].idle = 0;

        /* move all keys in the right to left by one item. */
        if (k != EVPOOL_SIZE-1 && pool[k+1].key != NULL) {
            sds save = pool[k].cached;
            memmove(pool+k, pool+k+1, sizeof(pool[0])*(EVPOOL_SIZE-k-1));
            pool[EVPOOL_SIZE-1].cached = save;
            pool[EVPOOL_SIZE-1].key = NULL;
            pool[EVPOOL_SIZE-1].idle = 0;
        }
        i++;
    }
}


typedef struct evictionPoolEntry POOL_RESULTS[EVPOOL_SIZE] ;
#define TEST_RESULTS(id,arr,size)  {test_hotpool_equal(&id, arr, size);sdsfree(s);}
void test_replaceKeyInPool() {
    int j, id = 0;
    sds s;
    struct evictionPoolEntry *ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    TestHotKeyPool = ep;

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 10, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{10, "abc"}};
        TEST_RESULTS(id, tmp, EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 50, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{50,"abc"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 40, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{40,"abc"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("111")), 0, 30, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{40,"abc"},{30,"111"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("222")), 0, 30, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{40,"abc"},{30,"222"}, {30,"111"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("333")), 0, 30, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{40,"abc"},{30, "333"},{30,"222"}, {30,"111"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 50, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{50,"abc"},{30, "333"},{30,"222"}, {30,"111"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 10, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{30, "333"},{30,"222"}, {30,"111"}, {10,"abc"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("222")), 0, 9, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{30, "333"},{30,"111"}, {10,"abc"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 13, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{30, "333"},{30,"111"}, {13,"abc"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("abc")), 0, 60, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{60,"abc"}, {30, "333"},{30,"111"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("555")), 0, 50, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{60,"abc"}, {50, "555"},{30, "333"},{30,"111"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("111")), 0, 55, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{60,"abc"},{55,"111"}, {50, "555"}, {30, "333"},{9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("111")), 0, 20, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{60,"abc"},{50, "555"}, {30, "333"},{20,"111"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("666")), 0, 70, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("888")), 0, 80, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{80,"888"},{70, "666"},{60,"abc"},{50, "555"}, {30, "333"},{20,"111"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("999")), 0, 90, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("xxx")), 0, 100, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("yyy")), 0, 110, HOT_POOL_TYPE);sdsfree(s);

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("zzz")), 0, 110, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("uuu")), 0, 120, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("vvv")), 0, 130, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("www")), 0, 140, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("ooo")), 0, 150, HOT_POOL_TYPE);sdsfree(s);
    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("ppp")), 0, 160, HOT_POOL_TYPE);sdsfree(s);

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("fff")), 0, 220, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{160,"ppp"},
                          {150,"ooo"}, {140,"www"}, {130,"vvv"},{120,"uuu"},{110,"zzz"},
                          {110,"yyy"},{100,"xxx"},{90,"999"}, {80,"888"}, {70, "666"},{60,"abc"},
                          {50, "555"}, {30, "333"},{20,"111"}, {9, "222"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("fff")), 0, 8, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{150,"ooo"}, {140,"www"},{130,"vvv"}, {120,"uuu"},{110,"zzz"},
                          {110,"yyy"}, {100,"xxx"},{90,"999"}, {80,"888"},{70, "666"}, {60,"abc"},
                          {50, "555"}, {30, "333"},{20,"111"}, {9, "222"},
                          {8,"fff"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    replaceKeyInPool(TestHotKeyPool, (s=sdsnew("bbb")), 0, 111, HOT_POOL_TYPE);
    { POOL_RESULTS tmp = {{140,"www"},{130,"vvv"},{120,"uuu"}, {111,"bbb"},{110,"zzz"},
                          {110,"yyy"},{100,"xxx"},{90,"999"}, {80,"888"},{70, "666"}, {60,"abc"},
                          {50, "555"}, {30, "333"},{20,"111"}, {9, "222"},
                          {8,"fff"}};
        TEST_RESULTS(id,tmp,EVPOOL_SIZE);
        replaceKeyInPool(TestHotKeyPool, (s=sdsnew("fff")), 0, 8, HOT_POOL_TYPE);
        TEST_RESULTS(id,tmp,EVPOOL_SIZE); }

    for (j = 0; j < EVPOOL_SIZE; j++) {
        if (ep[j].key != NULL && ep[j].key != ep[j].cached) {
            sdsfree(ep[j].key);
        }
        sdsfree(ep[j].cached);
    }
    zfree(ep);
}

void random_test_hotkeypool() {
    const char* key;
    int j = 0;
    while(1) {
        unsigned long long idle = random()%16;
        if (j == 0 || (j%5 != 0)) {
            removeHotKeys();
            key = keys[random()%(sizeof(keys)/sizeof(char*))];
        }
        sds s=sdsnew(key);

        printf("try insert key: %s, idle: %llu\n", s, idle);
        replaceKeyInPool(TestHotKeyPool, s, 0, idle, HOT_POOL_TYPE);
        sdsfree(s);
        j++;
    }
}

int main() {
    test_replaceKeyInPool();

    newHotKeyPool();
    freeHotKeyPool();

    newHotKeyPool();
    random_test_hotkeypool();
    freeHotKeyPool();
}
