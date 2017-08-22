//
// Created by ljc on 17-6-28.
//

#include "memprotect.h"

int protectMemWrite(void *p, int len)
{
    uint64_t addr = (uint64_t)p;
    if (p == NULL || len == 0) return -1;

    assert((addr % PAGE_SIZE ) == 0 && (len % PAGE_SIZE) == 0 && len != 0);
    return mprotect(p, len, PROT_READ);
}

int protectMemAccess(void *p, int len)
{
    uint64_t addr = (uint64_t)p;
    if (p == NULL || len == 0) return -1;

    assert((addr % PAGE_SIZE ) == 0 && (len % PAGE_SIZE) == 0 && len != 0);
    return mprotect(p, len, PROT_NONE);
}

int unprotectMemAccess(void *p, int len)
{
    uint64_t addr = (uint64_t)(p);
    if (p == NULL || len == 0) return -1;

    assert((addr % PAGE_SIZE ) == 0 && (len % PAGE_SIZE) == 0 && len != 0);
    return mprotect(p, PAGE_SIZE, PROT_READ | PROT_WRITE);
}