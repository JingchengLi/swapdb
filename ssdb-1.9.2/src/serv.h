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

namespace Replic {

    enum State {
        START = 0,
        TRANS,
        END
    };
}


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
        rState++;
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

class SSDBServer
{
public:
	SSDBServer(SSDB *ssdb, const Config &conf, NetworkServer *net);
	~SSDBServer();

	SSDBImpl *ssdb;
    RecordMutex<Mutex> transfer_mutex_record_;

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

