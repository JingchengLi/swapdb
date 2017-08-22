//
// Created by ljc on 17-6-28.
//

#ifndef __MEMPROTECT_H
#define __MEMPROTECT_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <limits.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <assert.h>
#include "sds.h"
#include "sdsalloc.h"

#define PAGE_SIZE (sysconf(_SC_PAGESIZE))

int protectMemWrite(void *p, int len);
int protectMemAccess(void *p, int len);
int unprotectMemAccess(void *p, int len);

#endif // __MEMPROTECT_H
