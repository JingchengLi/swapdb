/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_WORKER_H_
#define NET_WORKER_H_

#include <string>
#include "../util/thread.h"
#include "proc.h"

class Fdevents;

// WARN: pipe latency is about 20 us, it is really slow!
class ProcWorker : public WorkerPool<ProcWorker, ProcJob *>::Worker{
public:
	explicit ProcWorker(const std::string &name);
	~ProcWorker()= default;
	void init();
	int proc(ProcJob *job);
};

typedef WorkerPool<ProcWorker, ProcJob *> ProcWorkerPool;


class BackgroundThreadJob {
public:

	Context ctx;
	Link *client_link = nullptr;

	virtual int process() = 0;

	virtual int callback(NetworkServer *nets, Fdevents *fdes) = 0;

    virtual ~BackgroundThreadJob() = default;
};


class BackgroundThreadWorker : public WorkerPool<BackgroundThreadWorker, BackgroundThreadJob *>::Worker {
public:
	explicit BackgroundThreadWorker(const std::string &name){
		this->name = name;
	}
	~BackgroundThreadWorker() = default;
	void init();
	int proc(BackgroundThreadJob *job);
};

typedef WorkerPool<BackgroundThreadWorker, BackgroundThreadJob *> BackgroundThreadPool;

#endif
