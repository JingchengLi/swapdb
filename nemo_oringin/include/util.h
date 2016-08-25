#ifndef NEMO_UTIL_H
#define NEMO_UTIL_H

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <byteswap.h>

#ifndef htobe64
# if __BYTE_ORDER == __LITTLE_ENDIAN
#  define htobe64(x) bswap_64 (x)
# else
#  define htobe64(x)  (x)
# endif
#endif

#ifndef be64toh
# if __BYTE_ORDER == __LITTLE_ENDIAN
#  define be64toh(x) bswap_64 (x)
# else
#  define be64toh(x) (x)
# endif
#endif

namespace nemo {

int StrToUint64(const char *s, size_t slen, uint64_t *value);
int StrToInt64(const char *s, size_t slen, int64_t *value); 
int StrToInt32(const char *s, size_t slen, int32_t *val); 
int StrToUint32(const char *s, size_t slen, uint32_t *val); 
int StrToDouble(const char *s, size_t slen, double *dval);
int Int64ToStr(char* dst, size_t dstlen, int64_t svalue);

int do_mkdir(const char *path, mode_t mode);
int mkpath(const char *path, mode_t mode);

int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);

int is_dir(const char* filename);
int delete_dir(const char* dirname);
}
#endif
