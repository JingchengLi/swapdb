//
// Created by zts on 16-12-26.
//

#ifndef SSDB_ZMALLOC_H
#define SSDB_ZMALLOC_H

#define assert(_e) ((_e)?(void)0 : (exit(1)))
#define zmalloc(t) ((malloc(t)))
#define zrealloc(p,t) ((realloc(p,t)))
#define zfree(ptr) if (ptr == NULL) return; free(*ptr);ptr=NULL;

#endif //SSDB_ZMALLOC_H
