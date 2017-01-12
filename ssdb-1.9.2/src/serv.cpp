/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "version.h"
#include "util/log.h"
#include "util/strings.h"
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"
#include "ssdb/background_job.h"

DEF_PROC(get);
DEF_PROC(set);
DEF_PROC(setx);
DEF_PROC(psetx);
DEF_PROC(setnx);
DEF_PROC(getset);
DEF_PROC(getbit);
DEF_PROC(setbit);
DEF_PROC(countbit);
DEF_PROC(substr);
DEF_PROC(getrange);
DEF_PROC(strlen);
DEF_PROC(bitcount);
DEF_PROC(del);
DEF_PROC(incr);
DEF_PROC(decr);
DEF_PROC(scan);
DEF_PROC(keys);
DEF_PROC(exists);
DEF_PROC(multi_get);
DEF_PROC(multi_set);
DEF_PROC(multi_del);
DEF_PROC(ttl);
DEF_PROC(pttl);
DEF_PROC(expire);
DEF_PROC(pexpire);
DEF_PROC(expireat);
DEF_PROC(pexpireat);
DEF_PROC(persist);

DEF_PROC(hsize);
DEF_PROC(hget);
DEF_PROC(hset);
DEF_PROC(hdel);
DEF_PROC(hincr);
DEF_PROC(hdecr);
DEF_PROC(hgetall);
DEF_PROC(hscan);
DEF_PROC(hkeys);
DEF_PROC(hvals);
DEF_PROC(hexists);
DEF_PROC(multi_hexists);
DEF_PROC(multi_hsize);
DEF_PROC(multi_hget);
DEF_PROC(multi_hset);
DEF_PROC(multi_hdel);

DEF_PROC(sadd);
DEF_PROC(srem);
DEF_PROC(scard);
DEF_PROC(sdiff);
DEF_PROC(sdiffstore);
DEF_PROC(sinter);
DEF_PROC(sinterstore);
DEF_PROC(sismember);
DEF_PROC(smembers);
DEF_PROC(smove);
DEF_PROC(spop);
DEF_PROC(srandmember);
DEF_PROC(sunion);
DEF_PROC(sunionstore);
DEF_PROC(sscan);

DEF_PROC(zrank);
DEF_PROC(zrrank);
DEF_PROC(zrange);
DEF_PROC(zrrange);
DEF_PROC(zsize);
DEF_PROC(zget);
DEF_PROC(zset);
DEF_PROC(zdel);
DEF_PROC(zincr);
DEF_PROC(zdecr);
DEF_PROC(zfix);
DEF_PROC(zscan);
DEF_PROC(zkeys);
DEF_PROC(zcount);
DEF_PROC(zexists);
DEF_PROC(zremrangebyrank);
DEF_PROC(zremrangebyscore);
DEF_PROC(multi_zexists);
DEF_PROC(multi_zsize);
DEF_PROC(multi_zget);
DEF_PROC(multi_zset);
DEF_PROC(multi_zdel);
	
DEF_PROC(qsize);
DEF_PROC(qfront);
DEF_PROC(qback);
DEF_PROC(qpush);
DEF_PROC(qpush_front);
DEF_PROC(qpush_back);
DEF_PROC(qpop);
DEF_PROC(qpop_front);
DEF_PROC(qpop_back);
DEF_PROC(qtrim_front);
DEF_PROC(qtrim_back);
DEF_PROC(qfix);
DEF_PROC(qclear);
DEF_PROC(qslice);
DEF_PROC(qrange);
DEF_PROC(qget);
DEF_PROC(qset);

DEF_PROC(dump2);
DEF_PROC(sync140);
DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(dbsize);
DEF_PROC(compact);
DEF_PROC(clear_binlog);
DEF_PROC(flushdb);

DEF_PROC(dump);
DEF_PROC(restore);
DEF_PROC(select);
DEF_PROC(client);
DEF_PROC(quit);
DEF_PROC(replic);
DEF_PROC(sync150);

DEF_PROC(rr_dump);
DEF_PROC(rr_restore);


#define REG_PROC(c, f)     net->proc_map.set_proc(#c, f, proc_##c)

void SSDBServer::reg_procs(NetworkServer *net){
	REG_PROC(get, "rt");
	REG_PROC(set, "wt");
	REG_PROC(del, "wt");
	REG_PROC(setx, "wt");
	REG_PROC(psetx, "wt");
	REG_PROC(setnx, "wt");
	REG_PROC(getset, "wt");
	REG_PROC(getbit, "rt");
	REG_PROC(setbit, "wt");
	REG_PROC(countbit, "rt");
	REG_PROC(substr, "rt");
	REG_PROC(getrange, "rt");
	REG_PROC(strlen, "rt");
	REG_PROC(bitcount, "rt");
	REG_PROC(incr, "wt");
	REG_PROC(decr, "wt");
	REG_PROC(scan, "rt");
	REG_PROC(keys, "rt");
	REG_PROC(exists, "rt");
	REG_PROC(multi_get, "rt");
	REG_PROC(multi_set, "wt");
	REG_PROC(multi_del, "wt");
	REG_PROC(ttl, "rt");
	REG_PROC(pttl, "rt");
	REG_PROC(expire, "wt");
	REG_PROC(pexpire, "wt");
	REG_PROC(expireat, "wt");
	REG_PROC(pexpireat, "wt");
	REG_PROC(persist, "wt");

	REG_PROC(hsize, "rt");
	REG_PROC(hget, "rt");
	REG_PROC(hset, "wt");
	REG_PROC(hdel, "wt");
	REG_PROC(hincr, "wt");
	REG_PROC(hdecr, "wt");
	REG_PROC(hgetall, "rt");
	REG_PROC(hscan, "rt");
	REG_PROC(hkeys, "rt");
	REG_PROC(hvals, "rt");
	REG_PROC(hexists, "rt");
	REG_PROC(multi_hexists, "rt");
	REG_PROC(multi_hsize, "rt");
	REG_PROC(multi_hget, "rt");
	REG_PROC(multi_hset, "wt");
	REG_PROC(multi_hdel, "wt");

	REG_PROC(sadd, "wt");
    REG_PROC(srem, "wt");
	REG_PROC(scard, "rt");
    REG_PROC(sdiff, "rt");
    REG_PROC(sdiffstore, "wt");
    REG_PROC(sinter, "rt");
    REG_PROC(sinterstore, "wt");
    REG_PROC(sismember, "rt");
    REG_PROC(smembers, "rt");
    REG_PROC(smove, "wt");
    REG_PROC(spop, "wt");
    REG_PROC(srandmember, "rt");
    REG_PROC(sunion, "rt");
    REG_PROC(sunionstore, "wt");
    REG_PROC(sscan, "rt");

	// because zrank may be extremly slow, execute in a seperate thread
	REG_PROC(zrank, "rt");
	REG_PROC(zrrank, "rt");
	REG_PROC(zrange, "rt");
	REG_PROC(zrrange, "rt");
	REG_PROC(zsize, "rt");
	REG_PROC(zget, "rt");
	REG_PROC(zset, "wt");
	REG_PROC(zdel, "wt");
	REG_PROC(zincr, "wt");
	REG_PROC(zdecr, "wt");
	REG_PROC(zfix, "wt");
	REG_PROC(zscan, "rt");
	REG_PROC(zkeys, "rt");
	REG_PROC(zcount, "rt");
	REG_PROC(zremrangebyrank, "wt");
	REG_PROC(zremrangebyscore, "wt");
	REG_PROC(zexists, "rt");
	REG_PROC(multi_zexists, "rt");
	REG_PROC(multi_zsize, "rt");
	REG_PROC(multi_zget, "rt");
	REG_PROC(multi_zset, "wt");
	REG_PROC(multi_zdel, "wt");

	REG_PROC(qsize, "rt");
	REG_PROC(qfront, "rt");
	REG_PROC(qback, "rt");
	REG_PROC(qpush, "wt");
	REG_PROC(qpush_front, "wt");
	REG_PROC(qpush_back, "wt");
	REG_PROC(qpop, "wt");
	REG_PROC(qpop_front, "wt");
	REG_PROC(qpop_back, "wt");
	REG_PROC(qtrim_front, "wt");
	REG_PROC(qtrim_back, "wt");
	REG_PROC(qfix, "wt");
	REG_PROC(qclear, "wt");
	REG_PROC(qslice, "rt");
	REG_PROC(qrange, "rt");
	REG_PROC(qget, "rt");
	REG_PROC(qset, "wt");

	REG_PROC(dump, "wt"); //auctual read but ...
	REG_PROC(restore, "wt");
	REG_PROC(rr_dump, "wt"); //auctual read but ...
	REG_PROC(rr_restore, "wt");

	REG_PROC(select, "rt");
	REG_PROC(client, "r");
	REG_PROC(quit, "r");
	REG_PROC(replic, "wt");
	REG_PROC(sync150, "r");


	REG_PROC(clear_binlog, "wt");
	REG_PROC(flushdb, "wt");

	REG_PROC(dump2, "b");
	REG_PROC(sync140, "b");
	REG_PROC(info, "r");
	REG_PROC(version, "r");
	REG_PROC(dbsize, "rt");
	// doing compaction in a reader thread, because we have only one
	// writer thread(for performance reason); we don't want to block writes
	REG_PROC(compact, "rt");

}


SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net){
	this->ssdb = (SSDBImpl *)ssdb;
	this->meta = meta;

	net->data = this;
	this->reg_procs(net);

	int sync_speed = conf.get_num("replication.sync_speed");

	backend_dump = new BackendDump(this->ssdb);
	backend_sync = new BackendSync(this->ssdb, sync_speed);
	expiration = new ExpirationHandler(this->ssdb);


	{
		const Config *upstream_conf = conf.get("upstream");
		if(upstream_conf != NULL) {
			std::string ip = conf.get_str("upstream.ip");
			int port = conf.get_num("upstream.port");

			log_info("upstream: %s:%d", ip.c_str(), port);

			redisUpstream = new RedisUpstream(ip ,port);
			backgroundJob = new BackgroundJob(this);

		}
	}

	{ // slaves
		const Config *repl_conf = conf.get("replication");
		if(repl_conf != NULL){
			std::vector<Config *> children = repl_conf->children;
			for(std::vector<Config *>::iterator it = children.begin(); it != children.end(); it++){
				Config *c = *it;
				if(c->key != "slaveof"){
					continue;
				}
				std::string ip = c->get_str("ip");
				int port = c->get_num("port");
				if(ip == ""){
					ip = c->get_str("host");
				}
				if(ip == "" || port <= 0 || port > 65535){
					continue;
				}
				bool is_mirror = false;
				std::string type = c->get_str("type");
				if(type == "mirror"){
					is_mirror = true;
				}else{
					type = "sync";
					is_mirror = false;
				}
				
				std::string id = c->get_str("id");
				
				log_info("slaveof: %s:%d, type: %s", ip.c_str(), port, type.c_str());
				Slave *slave = new Slave(ssdb, meta, ip.c_str(), port, is_mirror);
				if(!id.empty()){
					slave->set_id(id);
				}
				slave->auth = c->get_str("auth");
				slave->start();
				slaves.push_back(slave);
			}
		}
	}


}

SSDBServer::~SSDBServer(){
	std::vector<Slave *>::iterator it;
	for(it = slaves.begin(); it != slaves.end(); it++){
		Slave *slave = *it;
		slave->stop();
		delete slave;
	}

	delete backend_dump;
	delete backend_sync;
	delete expiration;

	if (backgroundJob != nullptr) {
		delete backgroundJob;
	}
	if (redisUpstream != nullptr) {
		delete redisUpstream;
	}

	log_debug("SSDBServer finalized");
}

/*********************/

int proc_clear_binlog(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->binlogs->flush();
	resp->push_back("ok");
	return 0;
}

int proc_flushdb(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(serv->slaves.size() > 0 || serv->backend_sync->stats().size() > 0){
		resp->push_back("error");
		resp->push_back("flushdb is not allowed when replication is in use!");
		return 0;
	}
	serv->ssdb->flushdb();
	resp->push_back("ok");
	return 0;
}

int proc_select(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	return 0;
}


int proc_client(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	return 0;
}


int proc_quit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	//TODO quit support
	resp->push_back("ok");
	return 0;
}

int proc_dump2(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_dump->proc(link);
	return PROC_BACKEND;
}


int proc_restore(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
    CHECK_NUM_PARAMS(4);

	int64_t ttl = req[2].Int64();
	if (errno == EINVAL || ttl < 0){
		resp->push_back("error");
		return 0;
	}

    bool replace = false;
    if (req.size()>4) {
        std::string q4 = req[4].String();
        strtoupper(&q4);
        if (q4 == "REPLACE") {
            replace = true;
        } else {
            resp->push_back("error");
            return 0;
        }
    }

	std::string val;

	PTS(restore)
	int ret = serv->ssdb->restore(req[1], ttl, req[3], replace, &val);
	PTE(restore)

	if (ret > 0 && ttl > 0) {
		Locking l(&serv->expiration->mutex);
		ret = serv->expiration->expire(req[1], ttl, TimeUnit::Millisecond);
	}

	if (ret < 0) {
		log_info("%s : %s", hexmem(req[1].data(),req[1].size()).c_str(), hexmem(req[3].data(),req[3].size()).c_str());
	}

    resp->reply_get(ret, &val);
    return 0;
}

int proc_dump(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;

	PTS(dump)
	int ret = serv->ssdb->dump(req[1], &val);
//	log_info("len : %d",val.length());
	PTE(dump)

	resp->reply_get(ret, &val);
	return 0;
}



int proc_rr_restore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t ttl = req[2].Int64();
	if (errno == EINVAL || ttl < 0){
		resp->push_back("error");
		return 0;
	}

	bool replace = false;
	if (req.size()>4) {
		std::string q4 = req[4].String();
		strtoupper(&q4);
		if (q4 == "REPLACE") {
			replace = true;
		} else {
			resp->push_back("error");
			return 0;
		}
	}

	std::string val;

	PTST(rr_restore, 0.3)
	int ret = serv->ssdb->restore(req[1], ttl, req[3], replace, &val);
	PTE(rr_restore)

    if (ret > 0 && ttl > 0) {
		Locking l(&serv->expiration->mutex);
		ret = serv->expiration->expire(req[1], ttl, TimeUnit::Millisecond);
	}

	if (ret < 0) {
		log_info("%s : %s", hexmem(req[1].data(),req[1].size()).c_str(), hexmem(req[3].data(),req[3].size()).c_str());
	}

//    serv->ssdb->raw_set(encode_bqueue_key(COMMAND_REDIS_DEL, req[1]), "");

    BTask bTask(COMMAND_REDIS_DEL, req[1].String(), "");
    serv->bqueue.push(bTask);

//
//    PTST(backgroundJob_signal, 0.3)
//    serv->backgroundJob->queued++;
//	serv->backgroundJob->cv.signal();
//    PTE(backgroundJob_signal)

	resp->reply_get(ret, &val);
	return 0;
}


int proc_rr_dump(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string val = "OK";

//	resp->reply_get(ret, &val);

    PTST(rr_dump, 0.3)
    resp->reply_get(1, &val);
    PTE(rr_dump)


//	serv->ssdb->raw_set(encode_bqueue_key(COMMAND_REDIS_RESTROE, req[1]), "");
    BTask bTask(COMMAND_REDIS_RESTROE, req[1].String(), "");
    serv->bqueue.push(bTask);

//	serv->backgroundJob->queued++;
//	serv->backgroundJob->cv.signal();

	return 0;
}

int proc_sync140(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_sync->proc(link);
	return PROC_BACKEND;
}

int proc_compact(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->compact();
	resp->push_back("ok");
	return 0;
}

int proc_dbsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	uint64_t size = serv->ssdb->size();
	resp->push_back("ok");
	resp->push_back(str(size));
	return 0;
}

int proc_version(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back(SSDB_VERSION);
	return 0;
}

int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	resp->push_back("ok");
	resp->push_back("ssdb-server");
	resp->push_back("version");
	resp->push_back(SSDB_VERSION);
	resp->push_back("engine");
	resp->push_back(SSDB_ENGINE);
	{
		resp->push_back("links");
		resp->add(net->link_count);
	}
	{
		int64_t calls = 0;
		proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			calls += cmd->calls;
		}
		resp->push_back("total_calls");
		resp->add(calls);
	}
	
	{
		uint64_t size = serv->ssdb->size();
		resp->push_back("dbsize");
		resp->push_back(str(size));
	}

	{
		std::string s = serv->ssdb->binlogs->stats();
		resp->push_back("binlogs");
		resp->push_back(s);
	}
	{
		std::vector<std::string> syncs = serv->backend_sync->stats();
		std::vector<std::string>::iterator it;
		for(it = syncs.begin(); it != syncs.end(); it++){
			std::string s = *it;
			resp->push_back("replication");
			resp->push_back(s);
		}
	}
	{
		std::vector<Slave *>::iterator it;
		for(it = serv->slaves.begin(); it != serv->slaves.end(); it++){
			Slave *slave = *it;
			std::string s = slave->stats();
			resp->push_back("replication");
			resp->push_back(s);
		}
	}

	// todo check
#ifdef USE_LEVELDB
	if(req.size() == 1 || req[1] == "leveldb"){
#else
	if(req.size() == 1 || req[1] == "leveldb" || req[1] == "rocksdb"){
#endif
		std::vector<std::string> tmp = serv->ssdb->info();
		for(int i=0; i<(int)tmp.size(); i++){
			std::string block = tmp[i];
			resp->push_back(block);
		}
	}

	if(req.size() > 1 && req[1] == "cmd"){
		proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			resp->push_back("cmd." + cmd->name);
			char buf[128];
			snprintf(buf, sizeof(buf), "calls: %" PRIu64 "\ttime_wait: %.0f\ttime_proc: %.0f",
				cmd->calls, cmd->time_wait, cmd->time_proc);
			resp->push_back(buf);
		}
	}
	
	return 0;
}

static int ssdb_save_len(uint64_t len, std::string &res){
	unsigned char buf[2];
	size_t nwritten;

	if (len < (1<<6)) {
		/* Save a 6 bit len */
		buf[0] = (len&0xFF)|(RDB_6BITLEN<<6);
		res.append(1, buf[0]);
		nwritten = 1;
	} else if (len < (1<<14)) {
		/* Save a 14 bit len */
		buf[0] = ((len>>8)&0xFF)|(RDB_14BITLEN<<6);
		buf[1] = len&0xFF;
		res.append(1, buf[0]);
		res.append(1, buf[1]);
		nwritten = 2;
	} else if (len <= UINT32_MAX) {
		/* Save a 32 bit len */
		buf[0] = RDB_32BITLEN;
		res.append(1, buf[0]);
		uint32_t len32 = htobe32(len);
		res.append((char*)&len32, sizeof(uint32_t));
		nwritten = 1+4;
	} else {
		/* Save a 64 bit len */
		buf[0] = RDB_64BITLEN;
		res.append(1, buf[0]);
		len = htobe64(len);
		res.append((char*)&len, sizeof(uint64_t));
		nwritten = 1+8;
	}
	return (int)nwritten;
}

void* thread_replic(void *arg){
	SSDBServer *serv = (SSDBServer *)arg;
	std::vector<Slave_info>::iterator it = serv->slave_infos.begin();
	for (; it != serv->slave_infos.end(); ++it) {
		it->link = Link::connect((it->ip).c_str(), it->port);
		if(it->link == NULL){
			printf("fail to connect to slave ssdb! ip[%s] port[%d]\n", it->ip.c_str(), it->port);
			continue;
		}
		std::vector<std::string> req;
		req.push_back(std::string("sync150"));
		it->link->send(req);
		it->link->flush();
		it->link->response();
	}

	const leveldb::Snapshot *snapshot = nullptr;
	auto fit = std::unique_ptr<Iterator>(serv->ssdb->iterator("", "", -1, snapshot));
	while (fit->next()) {
		std::string key = fit->key().String();
		std::string val = fit->val().String();
        it = serv->slave_infos.begin();
		for (; it != serv->slave_infos.end(); ++it) {
			if (it->link != NULL) {
				std::string res;
				ssdb_save_len((uint64_t)(fit->key().size()), res);
				it->link->output->append(res.c_str(), (int)res.size());
				it->link->output->append(fit->key());
				res.clear();
				ssdb_save_len((uint64_t)(fit->val().size()), res);
				it->link->output->append(res.c_str(), (int)res.size());
				it->link->output->append(fit->val());

				if (it->link->output->size() > 512){
					it->link->flush();
                    it->link->response();
				}
			}
		}
	}

    it = serv->slave_infos.begin();
    for(; it != serv->slave_infos.end(); ++it){
        if (it->link != NULL ){
            if (it->link->output->size() > 0){
                it->link->flush();
                it->link->response();
                log_debug("flush data to slave via tcp");
            }
            delete it->link;
        }
    }
    serv->slave_infos.clear();

	serv->ssdb->ReleaseSnapshot(snapshot);

    if (serv->master_link != NULL){
        std::vector<std::string> response;
        response.push_back("replic finish");
        serv->master_link->send(response);
        serv->master_link->flush();
    }

	return (void *)NULL;
}

int proc_replic(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	if ((req.size() - 1) % 2 != 0){
		resp->push_back("wrong number of arguments");
		return 0;
	}
	for (int i = 1; i < req.size(); i += 2) {
		std::string ip = req[i].String();
		int port = req[i+1].Int();
		serv->slave_infos.push_back(Slave_info{ip, port, NULL});
	}
    serv->master_link = link;

//	breplication = true; //todo 设置全量复制开始标志

	pthread_t tid;
	int err = pthread_create(&tid, NULL, &thread_replic, serv);
	if(err != 0){
		log_fatal("can't create thread: %s", strerror(err));
		exit(0);
	}
	resp->push_back("ok");
	return 0;
}

int proc_sync150(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;

	int size = link->input->size();
	int orgi_size = size;
	char *head = link->input->data();
	int ret = serv->ssdb->parse_replic(head, size);
	link->input->decr(orgi_size - size);

    std::vector<std::string> request;
    request.push_back(std::string("ok"));
    link->send(request);
    link->flush();
    log_debug("slave response ok ");
	return 0;
}

