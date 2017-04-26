/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <net/redis/reponse_redis.h>
#include "worker.h"
#include "link.h"
#include "proc.h"
#include "../util/log.h"
#include "../include.h"

ProcWorker::ProcWorker(const std::string &name){
	this->name = name;
}

void ProcWorker::init(){
	log_debug("%s %d init", this->name.c_str(), this->id);
}

int ProcWorker::proc(ProcJob *job){
	const Request *req = job->req;
	
	proc_t p = job->cmd->proc;
	job->time_wait = 1000 * (millitime() - job->stime);
	job->result = (*p)(job->serv, job->link, *req, &job->resp);
	job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;

//	if (breplication && job->cmd->flags & Command::FLAG_WRITE){
		//todo 将req中的命令和参数保存值buffer中，待全量复制结束时发送值从ssdb
//	}

	if (job->resp.redisResponse != nullptr && job->link->redis != nullptr) {
		// raw redis protocol
		if(job->link->output->append(job->resp.redisResponse->toRedis()) == -1) {
			log_debug("job->link->send error");
			job->result = PROC_ERROR;

			delete job->resp.redisResponse;
			job->resp.redisResponse = nullptr;
			return 0;
		}

		delete job->resp.redisResponse;
		job->resp.redisResponse = nullptr;

	} else {
		if (job->resp.redisResponse != nullptr) {
			delete job->resp.redisResponse;
			job->resp.redisResponse = nullptr;
		}

		if(job->link->send(job->resp.resp) == -1){

			log_debug("job->link->send error");
			job->result = PROC_ERROR;
			return 0;
		}

	}


	//todo append custom reply
	if (job->link->append_reply) {
		if (!job->resp.resp.empty()) {
				if(job->link->send_append_res(job->resp.get_append_array()) == -1){

				log_debug("job->link->send_append_res error");
				job->result = PROC_ERROR;
				return 0;
			}
		}
	}

	int len = job->link->write();
	if(len < 0){
		log_debug("job->link->write error");
		job->result = PROC_ERROR;
	}

	return 0;
}
