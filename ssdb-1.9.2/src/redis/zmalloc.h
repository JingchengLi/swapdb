//
// Created by zts on 16-12-26.
//

#ifndef SSDB_ZMALLOC_H
#define SSDB_ZMALLOC_H

#define assert(_e) ((_e)?(void)0 : (exit(1)))

void *zmalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);


#endif //SSDB_ZMALLOC_H
