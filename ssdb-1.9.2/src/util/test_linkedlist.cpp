//
// Created by zts on 17-5-16.
//


#include "linked_list.hpp"
#include "log.h"

using namespace r2m;
using namespace std;

int main(int argc, char **argv) {

    int a = 0;
    int b = 1;
    int c = 2;

    LinkedList<int> list;


    list.push_back(&a);
    list.push_back(&b);
    list.push_back(&c);

    log_debug("start");

    list.remove(&b);
    log_debug("remove(&b)");

    auto iterator = list.iterator();
    while (auto item = iterator.next()) {
        log_debug("%d", *item->data);
    }

    list.remove(&a);
    log_debug("remove(&a)");

    iterator = list.iterator();
    while (auto item = iterator.next()) {
        log_debug("%d", *item->data);
    }


    list.remove(&c);
    log_debug("remove(&c)");

    iterator = list.iterator();
    while (auto item = iterator.next()) {
        log_debug("%d", *item->data);
    }

    log_debug("done");


}