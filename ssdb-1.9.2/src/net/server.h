/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_SERVER_H_
#define NET_SERVER_H_

#include "../include.h"
#include <string>
#include <vector>
#include <util/slowlog.h>

#include "fde.h"
#include "proc.h"
#include "worker.h"
#include "redis/transfer.h"

class Link;
class Config;
class IpFilter;
class Fdevents;

typedef std::vector<Link *> ready_list_t;

class NetworkServer
{
private:
	int tick_interval;
	int status_report_ticks;

	//Config *conf;
	Link *serv_link;
	Link *serv_socket;
	Fdevents *fdes;

	Link* accept_link(Link *link);
	int proc_result(ProcJob *job, ready_list_t *ready_list);
	int proc_client_event(const Fdevent *fde, ready_list_t *ready_list);

	int proc(ProcJob *job);

	int num_readers;
	int num_writers;
	int num_transfers = 5;
	int num_background = 3;

	ProcWorkerPool *writer;
	ProcWorkerPool *reader;

	NetworkServer();

	void cleanup_cursor();

protected:
	void usage(int argc, char **argv);

public:
	TransferWorkerPool *redis;
	BackgroundThreadPool *background;

	IpFilter *ip_filter;
	void *data;
	ProcMap proc_map;
	int link_count;
	bool need_auth;
	std::string password;

	~NetworkServer();
	
	// could be called only once
	static NetworkServer* init(const char *conf_file, int num_readers=-1, int num_writers=-1);
	static NetworkServer* init(const Config &conf, int num_readers=-1, int num_writers=-1);
	void serve();

	Slowlog slowlog;

};


#endif
