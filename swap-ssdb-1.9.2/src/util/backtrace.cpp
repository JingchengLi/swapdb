//
// Created by zts on 17-4-20.
//

#include "backtrace.h"
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>

#define DUMP_STACK_DEPTH_MAX 16

void dump_trace() {
    void *stack_trace[DUMP_STACK_DEPTH_MAX] = {0};
    char **stack_strings = NULL;
    int stack_depth = 0;
    int i = 0;

    stack_depth = backtrace(stack_trace, DUMP_STACK_DEPTH_MAX);
    stack_strings = backtrace_symbols(stack_trace, stack_depth);
    if (NULL == stack_strings) {
        printf("Dump Stack Trace Failed!!\r\n");
        return;
    }

    printf("Stack Trace: \n");
    for (i = 0; i < stack_depth; ++i) {
        printf(" [%d] %s \n", i, stack_strings[i]);
    }

    /* 获取函数名称时申请的内存需要自行释放 */
    free(stack_strings);
    stack_strings = NULL;

    return;
}