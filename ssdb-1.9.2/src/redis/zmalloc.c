//
// Created by zts on 17-1-19.
//

#include <stddef.h>
#include <malloc.h>
#include "zmalloc.h"

void *zmalloc(size_t size) {
    void *ptr = malloc(size);
    return ptr;
}

void *zrealloc(void *ptr, size_t size) {
    if (ptr == NULL) return zmalloc(size);
    void * newptr = realloc(ptr,size);
    return newptr;
}

void zfree(void *ptr) {
    if (ptr == NULL) return;
    free(ptr);
}