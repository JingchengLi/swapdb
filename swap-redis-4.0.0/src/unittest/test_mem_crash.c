#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <malloc.h>
#include <sys/mman.h>

#define PAGE_SIZE (sysconf(_SC_PAGESIZE))

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

int main(int argc, char *argv[])
{
    char* ptr = memalign(PAGE_SIZE, PAGE_SIZE);
    printf("page size:%ld, ptr:%lu\n", PAGE_SIZE, (uint64_t)ptr);
    printf("ptr %% pagesize == 0 ? %s\n", ((uint64_t)ptr % PAGE_SIZE) == 0 ? "yes" : "no");

    protectMemWrite(ptr,PAGE_SIZE);
    printf("will crash By SIGSEGV\n");
    *(ptr+1) = '9';
    unprotectMemAccess(ptr,PAGE_SIZE);
}

