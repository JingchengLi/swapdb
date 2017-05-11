//
// Created by zts on 17-5-10.
//

#ifndef SSDB_CONTEXT_H
#define SSDB_CONTEXT_H

//#include <net/server.h>


class RepopContext{
public:
    uint64_t    id = 0;
    uint64_t    timestamp = 0;

    void reset () {
        id = 0;
        timestamp = 0;
    }
};


class NetworkServer;
//class SSDBServer;

class Context
{
public:
    NetworkServer *net = nullptr;

    RepopContext recievedRepopContext;
    RepopContext commitedRepopContext;

};


#endif //SSDB_CONTEXT_H
