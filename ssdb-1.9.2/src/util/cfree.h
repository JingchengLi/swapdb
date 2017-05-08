//
// Created by zts on 17-5-8.
//

#ifndef SSDB_CFREE_H
#define SSDB_CFREE_H

#include <malloc.h>

template <typename T>
struct cfree_delete
{
    void operator()(T* x) { free(x); }
};



#endif //SSDB_CFREE_H
