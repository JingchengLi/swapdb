/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SERVER_H_
#define SSDB_SERVER_H_

#include "include.h"
#include <map>
#include <vector>
#include <string>
#include <util/dump_data.h>
#include "ssdb/ssdb_impl.h"
#include "ssdb/ttl.h"
#include "net/server.h"
#include "SSDB_client.h"
#include "net/link.h"
#include "util/error.h"
#include "util/internal_error.h"


class ReplicationState {
public:
    const static int START = 0;
    const static int TRANS = 1;
    const static int END = 2;
    std::vector<std::string> States = {"START", "TRANS", "END"};

public:

    const leveldb::Snapshot *rSnapshot = nullptr;
    Mutex rMutex;

    int rState = ReplicationState::START;
    uint64_t numStarted = 0;
    uint64_t numFinished = 0;
    uint64_t numFailed = 0;

    void startReplic() {
        rState = ReplicationState::START;
        numStarted++;
    }

    void finishReplic(bool ok) {
        if (ok) {
            numFinished++;
        } else {
            numFailed++;
        }
        if (numStarted == (numFinished + numFailed)) {
            rState = ReplicationState::END;
        }
    }

    void resetReplic() {
        rState = ReplicationState::START;
        numStarted = 0;
        numFinished = 0;
        numFailed = 0;
    }

    bool inTransState() {
        return rState == ReplicationState::TRANS;
    }

};

class ScanParams {
public:
    std::string pattern = "*";
    uint64_t limit = 10;
};

class SSDBServer
{
public:
	SSDBServer(SSDB *ssdb, const Config &conf, NetworkServer *net);
	~SSDBServer();

	SSDBImpl *ssdb;
    RecordMutex<Mutex> transfer_mutex_record_;

    const Config &conf;
    
    HostAndPort redisConf;

    ReplicationState replicState;

private:
    void reg_procs(NetworkServer *net);

};


#define CHECK_NUM_PARAMS(n) do{ \
		if(req.size() < n){ \
			resp->push_back("client_error"); \
			resp->push_back("ERR wrong number of arguments"); \
			return 0; \
		} \
	}while(0)

#endif


inline int prepareForScanParams(std::vector<Bytes> req, int startIndex,  ScanParams &scanParams) {

    std::vector<Bytes>::const_iterator it = req.begin() + startIndex;
    for(; it != req.end(); it += 2){
        std::string key = (*it).String();
        strtolower(&key);

        if (key=="match") {
            scanParams.pattern = (*(it+1)).String();
        } else if (key=="count") {
            scanParams.limit =  (*(it+1)).Uint64();
            if (errno == EINVAL){
                return INVALID_INT;
            }
        } else {
            return SYNTAX_ERR;
        }
    }

    return 0;
}
