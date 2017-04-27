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
#include "backend_dump.h"
#include "net/server.h"
#include "SSDB_client.h"
#include "net/link.h"
#include <util/error.h>


enum ReplicState{
	REPLIC_START = 0,
	REPLIC_TRANS	,
	REPLIC_END
};

class SSDBServer
{
private:
	void reg_procs(NetworkServer *net);

	SSDB *meta;

public:
	SSDBImpl *ssdb;
	BackendDump *backend_dump;

    HostAndPort redisConf;

	RecordMutex<Mutex> transfer_mutex_record_;


	const leveldb::Snapshot* replicSnapshot;
    Mutex         			 replicMutex;
    enum ReplicState 		 replicState;
	int 					 replicNumStarted;
	int 					 replicNumFinished;

	SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net);
	~SSDBServer();

};


#define CHECK_NUM_PARAMS(n) do{ \
		if(req.size() < n){ \
			resp->push_back("client_error"); \
			resp->push_back("ERR wrong number of arguments"); \
			return 0; \
		} \
	}while(0)

#endif

