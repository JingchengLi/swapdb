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

    bool operator<(const RepopContext &b) const {
        if (this->timestamp < b.timestamp) {
            return true;
        } else if (this->timestamp == b.timestamp) {
            return this->id < b.id;
        } else {
            return false;
        }
    }

    bool operator!=(uint64_t id) const {
        return this->id != id;
    }

    bool operator==(uint64_t id) const {
        return this->id == id;
    }

    std::string toString() {
        return "repopid " + str(timestamp) + " " + str(id);
    }
};


class NetworkServer;
//class SSDBServer;

class Context {
public:
    NetworkServer *net = nullptr;

    RepopContext currentSeqCnx;
    RepopContext lastSeqCnx;


    bool checkKey = false;
    bool firstbatch = true;
    bool replLink = false;

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


        if (replLink) {
            lastSeqCnx = currentSeqCnx;
        }

        vec.emplace_back(std::move(lastSeqCnx.toString()));

        return vec;
    }

};


#endif //SSDB_CONTEXT_H
