//
// Created by zts on 17-1-19.
//

#include <stddef.h>
#include <malloc.h>
#include <zconf.h>
#include <fcntl.h>
#include <stdlib.h>
#include "zmalloc.h"
#include "lzfP.h"

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

size_t zmalloc_get_rss(void) {
    int page = sysconf(_SC_PAGESIZE);
    size_t rss;
    char buf[4096];
    char filename[256];
    int fd, count;
    char *p, *x;

    snprintf(filename,256,"/proc/%d/stat",getpid());
    if ((fd = open(filename,O_RDONLY)) == -1) return 0;
    if (read(fd,buf,4096) <= 0) {
        close(fd);
        return 0;
    }
    close(fd);

    p = buf;
    count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
    while(p && count--) {
        p = strchr(p,' ');
        if (p) p++;
    }
    if (!p) return 0;
    x = strchr(p,' ');
    if (!x) return 0;
    *x = '\0';

    rss = strtoll(p,NULL,10);
    rss *= page;
    return rss;
}