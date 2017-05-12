//
// Created by zts on 17-5-10.
//

#ifndef SSDB_CONTEXT_H
#define SSDB_CONTEXT_H

//#include <net/server.h>
#include <string>
#include <vector>
#include "util/strings.h"


class RepopContext {
public:
    uint64_t id = 0;
    uint64_t timestamp = 0;

    void reset() {
        id = 0;
        timestamp = 0;
    }
};


class NetworkServer;
//class SSDBServer;

class Context {
public:
    NetworkServer *net = nullptr;

    RepopContext recievedRepopContext;
    RepopContext commitedRepopContext;


    bool checkKey = false;
    bool firstbatch = true;

    void mark_check() {
        checkKey = true;
    }

    void reset() {
        checkKey = false;
        firstbatch = true;
    }

    bool isFirstbatch() const {
        return firstbatch;
    }

    void setFirstbatch(bool firstbatch) {
        Context::firstbatch = firstbatch;
    }

    std::vector<std::string> get_append_array() {
        std::vector<std::string> vec;

        if (checkKey) {
            vec.emplace_back("check 1");
        } else {
            vec.emplace_back("check 0");
        }

        vec.push_back("repopid " + str(commitedRepopContext.timestamp) + " " + str(commitedRepopContext.id));

        return vec;
    }

};


#endif //SSDB_CONTEXT_H
