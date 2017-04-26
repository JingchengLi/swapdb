/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "server.h"
#include "../util/strings.h"
#include "../util/file.h"
#include "../util/config.h"
#include "../util/log.h"
#include "../util/ip_filter.h"
#include "link.h"
#include "fde.h"
#include <vector>
#include <execinfo.h>
#include <serv.h>

static DEF_PROC(ping);
static DEF_PROC(info);
static DEF_PROC(auth);


#define TICK_INTERVAL          100 // ms
#define STATUS_REPORT_TICKS    (300 * 1000/TICK_INTERVAL) // second
#define CURSOR_CLEANUP_TICKS    (60 * 1000/TICK_INTERVAL) // second

static const int TRANSFER_THREADS = 5;
static const int READER_THREADS = 10;
static const int WRITER_THREADS = 1;  // 必须为1, 因为某些写操作依赖单线程

volatile bool quit = false;
volatile uint32_t g_ticks = 0;


//void sig_signal_handler(int sig){
void sig_signal_handler(int sig, siginfo_t * info, void * ucontext) {
//    ucontext_t *uc = (ucontext_t*) ucontext;
    log_error("SSDB %s crashed by signal: %d", SSDB_VERSION, sig);


    void *array[20];
    int size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 20);

    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d (%s)\n", sig, strsignal(sig));
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    backtrace_symbols_fd(array, size, log_fd()->_fileno);
//	abort();
//	exit(sig);
	close(log_fd()->_fileno);
}

void signal_handler(int sig){
	switch(sig){
		case SIGKILL:
		case SIGTERM:
		case SIGINT:{
			quit = true;
			break;
		}
		case SIGALRM:{
			g_ticks ++;
			break;
		}
	}
}

NetworkServer::NetworkServer(){
	num_readers = READER_THREADS;
	num_writers = WRITER_THREADS;
	num_transfers = TRANSFER_THREADS;

	tick_interval = TICK_INTERVAL;
	status_report_ticks = STATUS_REPORT_TICKS;

	//conf = NULL;
	serv_link = NULL;
	serv_socket = nullptr;
	link_count = 0;

	fdes = new Fdevents();
	ip_filter = new IpFilter();

	// add built-in procs, can be overridden
	proc_map.set_proc("ping", "r", proc_ping);
	proc_map.set_proc("info", "r", proc_info);
	proc_map.set_proc("auth", "r", proc_auth);


	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, signal_handler);
	signal(SIGQUIT, signal_handler);
	signal(SIGKILL, signal_handler);
	signal(SIGTERM, signal_handler);
//	signal(SIGSEGV, sig_signal_handler);
#ifndef __CYGWIN__
	signal(SIGALRM, signal_handler);
	{
		struct itimerval tv;
		tv.it_interval.tv_sec = (TICK_INTERVAL / 1000);
		tv.it_interval.tv_usec = (TICK_INTERVAL % 1000) * 1000;
		tv.it_value.tv_sec = 1;
		tv.it_value.tv_usec = 0;
		setitimer(ITIMER_REAL, &tv, NULL);
	}
#endif

    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sig_signal_handler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);

    return;
}
	
NetworkServer::~NetworkServer(){
	//delete conf;
	delete serv_link;
	if (serv_socket != nullptr ) {
		delete serv_socket;
	}
	delete fdes;
	delete ip_filter;

	writer->stop();
	delete writer;
	reader->stop();
	delete reader;

	redis->stop();
	delete redis;

	log_info("NetworkServer finalized");
}

NetworkServer* NetworkServer::init(const char *conf_file, int num_readers, int num_writers){
	if(!is_file(conf_file)){
		fprintf(stderr, "'%s' is not a file or not exists!\n", conf_file);
		exit(1);
	}

	Config *conf = Config::load(conf_file);
	if(!conf){
		fprintf(stderr, "error loading conf file: '%s'\n", conf_file);
		exit(1);
	}
	{
		std::string conf_dir = real_dirname(conf_file);
		if(chdir(conf_dir.c_str()) == -1){
			fprintf(stderr, "error chdir: %s\n", conf_dir.c_str());
			exit(1);
		}
	}
	NetworkServer* serv = init(*conf, num_readers, num_writers);
	delete conf;
	return serv;
}

NetworkServer* NetworkServer::init(const Config &conf, int num_readers, int num_writers){
	static bool inited = false;
	if(inited){
		return NULL;
	}
	inited = true;
	
	NetworkServer *serv = new NetworkServer();
	if(num_readers >= 0){
		serv->num_readers = num_readers;
	}
	if(num_writers >= 0){
		serv->num_writers = num_writers;
	}
	// init ip_filter
	{
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "allow"){
					const char *ip = (*it)->str();
					log_info("    allow %s", ip);
					serv->ip_filter->add_allow(ip);
				}
				if((*it)->key == "deny"){
					const char *ip = (*it)->str();
					log_info("    deny %s", ip);
					serv->ip_filter->add_deny(ip);
				}
			}
		}
	}
	
	{ // server
		const char *ip = conf.get_str("server.ip");
		int port = conf.get_num("server.port");
		if(ip == NULL || ip[0] == '\0'){
			ip = "127.0.0.1";
		}
//
		serv->serv_link = Link::listen(ip, port);
		if(serv->serv_link == NULL){
			log_fatal("error opening server socket! %s", strerror(errno));
			fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
			exit(1);
		}

		const char *unixsocketfile = conf.get_str("server.file");
		if( strlen(unixsocketfile) != 0) {
			serv->serv_socket = Link::unixsocket(unixsocketfile);
			if(serv->serv_socket == NULL){
				log_fatal("error opening server socket! %s", strerror(errno));
				fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
				exit(1);
			}
			serv->serv_socket->noblock();
			log_info("server listen on %s", unixsocketfile);

		}

		// see UNP
		// if client send RST between server's calls of select() and accept(),
		// accept() will block until next connection.
		// so, set server socket nonblock.
		serv->serv_link->noblock();
		log_info("server listen on %s:%d", ip, port);

		std::string password;
		password = conf.get_str("server.auth");
		if(password.size() && (password.size() < 32 || password == "very-strong-password")){
			log_fatal("weak password is not allowed!");
			fprintf(stderr, "WARNING! Weak password is not allowed!\n");
			exit(1);
		}
		if(password.empty()){
			log_info("auth: off");
		}else{
			log_info("auth: on");
		}
		serv->need_auth = false;		
		if(!password.empty()){
			serv->need_auth = true;
			serv->password = password;
		}
	}
	return serv;
}

void NetworkServer::serve(){
	writer = new ProcWorkerPool("writer");
	writer->start(num_writers);
	reader = new ProcWorkerPool("reader");
	reader->start(num_readers);

	redis = new TransferWorkerPool("transfer");
	redis->start(num_transfers);

	ready_list_t ready_list;
	ready_list_t ready_list_2;
	ready_list_t::iterator it;
	const Fdevents::events_t *events;

	fdes->set(serv_link->fd(), FDEVENT_IN, 0, serv_link);
	if (serv_socket != nullptr) {
		fdes->set(serv_socket->fd(), FDEVENT_IN, 0, serv_socket);
	}
	fdes->set(this->reader->fd(), FDEVENT_IN, 0, this->reader);
	fdes->set(this->writer->fd(), FDEVENT_IN, 0, this->writer);
	fdes->set(this->redis->fd(), FDEVENT_IN, 0, this->redis);
	{
		SSDBServer *serv = (SSDBServer *)(this->data);
		fdes->set(serv->fds[0], FDEVENT_IN, 160, NULL);
	}

	uint32_t status_ticks = g_ticks;
	uint32_t cursor_ticks = g_ticks;

	while(!quit){
		double loop_stime = millitime();

		// status report
		if((uint32_t)(g_ticks - status_ticks) >= STATUS_REPORT_TICKS){
			status_ticks = g_ticks;
			log_info("server running, links: %d", this->link_count);
		}

		if((uint32_t)(g_ticks - cursor_ticks) >= CURSOR_CLEANUP_TICKS){
			cursor_ticks = g_ticks;
			Command *cmd = proc_map.get_proc("cursor_cleanup");
			if(!cmd){
				log_fatal("");
			} else {
				Request request;
				Response response;
				request.push_back("cursor_cleanup");
				request.push_back("1000");
				cmd->proc(this, nullptr, request, &response);
			}
		}

		SSDBServer *serv = (SSDBServer *)(this->data);
		/*static int nLoopNum = 0;
		pthread_mutex_lock(&serv->mutex);
		if (serv->ReplicState == REPLIC_END){
			pthread_mutex_unlock(&serv->mutex);
			nLoopNum++;
			if (nLoopNum == 10){
				Command *cmd = proc_map.get_proc("rr_del_snapshot");
				if(!cmd){
					log_fatal("not found rr_del_snapshot command");
				} else {
					Request request;
					Response response;
//					cmd->proc(this, nullptr, request, &response);
				}
				nLoopNum = 0;
			}
		} else{
			pthread_mutex_unlock(&serv->mutex);
			nLoopNum = 0;
		}
*/
		ready_list.swap(ready_list_2);
		ready_list_2.clear();
		
		if(!ready_list.empty()){
			// ready_list not empty, so we should return immediately
			events = fdes->wait(0);
		}else{
			events = fdes->wait(50);
		}
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}
		
		for(int i=0; i<(int)events->size(); i++){
			const Fdevent *fde = events->at(i);
			if(fde->data.ptr == serv_link){
				Link *link = accept_link(serv_link);
				if(link){
					this->link_count ++;
					log_debug("new link from %s:%d, fd: %d, links: %d",
						link->remote_ip, link->remote_port, link->fd(), this->link_count);
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}else{
					log_debug("accept return NULL");
				}
			}else if(fde->data.ptr == serv_socket){
				Link *link = accept_link(serv_socket);
				if(link){
                    link->append_reply = true;
					this->link_count ++;
					log_debug("new link from %s:%d, fd: %d, links: %d",
							  link->remote_ip, link->remote_port, link->fd(), this->link_count);
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}else{
					log_debug("accept return NULL");
				}
			}else if(fde->data.ptr == this->redis){
					TransferWorkerPool *worker = (TransferWorkerPool *)fde->data.ptr;
					TransferJob *job = NULL;
					if(worker->pop(&job) == 0){
						log_fatal("reading result from workers error!");
						exit(0);
					}
//
//					int qsize = worker->queued();
//					if (qsize > 500) {
//						log_error("BackgroundJob queue size is now : %d", qsize);
//					} else if (qsize > 218) {
//						log_warn("BackgroundJob queue size is now : %d", qsize);
//					} else if (qsize > 50) {
//						log_info("BackgroundJob queue size is now : %d", qsize);
//					} else if (qsize > 6) {
//						log_debug("BackgroundJob queue size is now : %d", qsize);
//					}
//
//					log_debug("job process done %s", job->dump().c_str());

					if (job != NULL) {
						delete job;
					}

			}else if(fde->data.ptr == this->reader || fde->data.ptr == this->writer){
				ProcWorkerPool *worker = (ProcWorkerPool *)fde->data.ptr;
				ProcJob *job = NULL;
				if(worker->pop(&job) == 0){
					log_fatal("reading result from workers error!");
					exit(0);
				}
				if(proc_result(job, &ready_list) == PROC_ERROR){
					//
				}
			} else if (fde->data.num == 150){
				Link* link = (Link*)fde->data.ptr;
				Command *cmd = proc_map.get_proc("sync150");
				if(!cmd){
					link->output->append("client_error");
					link->flush();
					continue;
				}

                int len = link->read();
                if (link->input->size() > 0) {
                    proc_t p = cmd->proc;
                    const Request req;
					Response resp;
                    int result = (*p)(this, link, req, &resp);
					if (resp.size() > 0) {
						link->output->append(resp.resp[0]);
						link->write();
						log_debug("reply replic finish ok!");
					}
                }

                if(len <= 0){
                    double serv_time = millitime() - link->create_time;
                    log_debug("fd: %d, read: %d, delete link, s:%.3f", link->fd(), len, serv_time);
                    this->link_count--;
                    fdes->del(link->fd());
                    delete link;
                    continue;
                }
            } else if (fde->data.num == 160){
                char buf[1] = {0};
                int n = ::read(serv->fds[0], buf, 1);
                if(n < 0){
                    if(errno == EINTR){
                        continue;
                    }else{
                        log_error("");
                        continue;
                    }
                }else if(n == 0){
                    log_error("");
                    continue;
                }
                serv->mutex_finish.lock();
                Slave_info slave = serv->slave_finish.front();
                serv->slave_finish.pop();
                serv->mutex_finish.unlock();
                std::vector<std::string> response;
                response.push_back("ok");
                response.push_back("rr_transfer_snapshot finished");
                log_debug("before send finish rr_link address:%lld", slave.master_link);
                if (slave.master_link != NULL){
                    slave.master_link->send(response);
                    if (slave.master_link->append_reply) {
                        response.clear();
                        response.push_back("check 0");
                        slave.master_link->send_append_res(response);
                    }
                    if (slave.master_link->write() <= 0){
                        log_error("The link write error, delete link! fd:%d", slave.master_link->fd());
                        this->link_count --;
                        fdes->del(slave.master_link->fd());
                        delete slave.master_link;
                    } else{
                        slave.master_link->noblock(true);
                        fdes->set(slave.master_link->fd(), FDEVENT_IN, 1, slave.master_link);
                    }
                } else{
                    log_error("The link from redis is off!");
                }
            } else{
                proc_client_event(fde, &ready_list);
            }
        }

		for(it = ready_list.begin(); it != ready_list.end(); it ++){
			Link *link = *it;
			if(link->error()){
				this->link_count --;
				fdes->del(link->fd());
				delete link;
                link = NULL;
				continue;
			}

			const Request *req = link->recv();
			if(req == NULL){
				log_warn("fd: %d, link parse error, delete link", link->fd());
                log_debug("error data length: %d  error data: %s", link->input->size(), hexmem(link->input->data(), link->input->size()).c_str());
				this->link_count --;
				fdes->del(link->fd());
				delete link;
                link = NULL;
				continue;
			}
			if(req->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
                log_debug("fd: %d, req is empty ", link->fd());
				continue;
			}


			link->active_time = millitime();

			ProcJob *job = new ProcJob();
			job->link = link;
			job->req = link->last_recv();
			int result = this->proc(job);
			if(result == PROC_THREAD){
				log_debug("[receive] req: %s", serialize_req(*job->req).c_str());
				fdes->del(link->fd());
			}else if(result == PROC_BACKEND){
				fdes->del(link->fd());
				this->link_count --;
			}else{
				if(proc_result(job, &ready_list_2) == PROC_ERROR){
					//
				}
			}
		} // end foreach ready link

		double loop_time = millitime() - loop_stime;
		if(loop_time > 0.5){
			log_warn("long loop time: %.3f", loop_time);
		}
	}
}

Link* NetworkServer::accept_link(Link *l){
	Link *link = l->accept();
	if(link == NULL){
		log_error("accept failed! %s", strerror(errno));
		return NULL;
	}
	if(!ip_filter->check_pass(link->remote_ip)){
		log_debug("ip_filter deny link from %s:%d", link->remote_ip, link->remote_port);
		delete link;
		return NULL;
	}
				
	link->nodelay();
	link->noblock();
	link->create_time = millitime();
	link->active_time = link->create_time;
	return link;
}

int NetworkServer::proc_result(ProcJob *job, ready_list_t *ready_list){
	Link *link = job->link;
	int result = job->result;
			
	if(log_level() >= Logger::LEVEL_DEBUG){
        auto dreply = job->resp.get_append_array();
		log_debug("[result] w:%.3f,p:%.3f, req: %s, resp: %s, dreply: %s",
			job->time_wait, job->time_proc,
			serialize_req(*job->req).c_str(),
			serialize_req(job->resp.resp).c_str(),
			serialize_req(dreply).c_str());
	}
	if(job->cmd){
		job->cmd->calls += 1;
		job->cmd->time_wait += job->time_wait;
		job->cmd->time_proc += job->time_proc;
	}

	slowlog.pushEntryIfNeeded(job->req, (int64_t) job->time_proc);

	if(result == PROC_ERROR){

		std::string error_cmd = "cmd: ";
		for(Request::const_iterator it=job->req->begin(); it!=job->req->end(); it++){
			const Bytes &key = *it;
			error_cmd.append(hexmem(key.data(),key.size()));
			error_cmd.append(" ");
		}

		error_cmd.append(" resp: ");
		for(std::vector<std::string>::const_iterator it=job->resp.resp.begin(); it!=job->resp.resp.end(); it++){
			const std::string& res = *it;
 			error_cmd.append(hexmem(res.data(),res.size()));
			error_cmd.append(" ");
		}

		log_info("fd: %d, proc error, delete link, %s", link->fd(), error_cmd.c_str());
		delete job;
		goto proc_err;
	}

	delete job;

	if(!link->output->empty()){
		int len = link->write();
		//log_debug("write: %d", len);
		if(len < 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			goto proc_err;
		}
	}

	if(!link->output->empty()){
		fdes->set(link->fd(), FDEVENT_OUT, 1, link);
	}
	if(link->input->empty()){
		fdes->set(link->fd(), FDEVENT_IN, 1, link);
	}else{
		fdes->clr(link->fd(), FDEVENT_IN);
		ready_list->push_back(link);
	}

	return PROC_OK;

proc_err:
	this->link_count --;
	fdes->del(link->fd());
	delete link;
	return PROC_ERROR;
}

/*
event:
	read => ready_list OR close
	write => NONE
proc =>
	done: write & (read OR ready_list)
	async: stop (read & write)
	
1. When writing to a link, it may happen to be in the ready_list,
so we cannot close that link in write process, we could only
just mark it as closed.

2. When reading from a link, it is never in the ready_list, so it
is safe to close it in read process, also safe to put it into
ready_list.

3. Ignore FDEVENT_ERR

A link is in either one of these places:
	1. ready list
	2. async worker queue
So it safe to delete link when processing ready list and async worker result.
*/
int NetworkServer::proc_client_event(const Fdevent *fde, ready_list_t *ready_list){
	Link *link = (Link *)fde->data.ptr;
	if(fde->events & FDEVENT_IN){
		ready_list->push_back(link);
		if(link->error()){
			return 0;
		}
		int len = link->read();
		//log_debug("fd: %d read: %d", link->fd(), len);
		if(len <= 0){
			double serv_time = millitime() - link->create_time;
			log_debug("fd: %d, read: %d, delete link, s:%.3f, e:%d, f:%d", link->fd(), len, serv_time, fde->events, fde->s_flags);
			link->mark_error();
			return 0;
		}
	}
	if(fde->events & FDEVENT_OUT){
		if(link->error()){
			return 0;
		}
		int len = link->write();
		if(len <= 0){
			log_debug("fd: %d, write: %d, delete link, e:%d, f:%d", link->fd(), len, fde->events, fde->s_flags);
			link->mark_error();
			return 0;
		}
		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
		}
	}
	return 0;
}

int NetworkServer::proc(ProcJob *job){
	job->serv = this;
	job->result = PROC_OK;
	job->stime = millitime();

	const Request *req = job->req;

	do{
		// AUTH
		if(this->need_auth && job->link->auth == false && req->at(0) != "auth"){
			job->resp.push_back("noauth");
			job->resp.push_back("authentication required");
			break;
		}

		if (req->at(0) == "sync150") {
			int sock = job->link->fd();
			fdes->del(sock);
			fdes->set(sock, FDEVENT_IN, 150, job->link);

			SSDBServer *serv = (SSDBServer *)(this->data);
			if (serv->ssdb->expiration){
				serv->ssdb->expiration->stop();
			}
			serv->ssdb->stop();

			Command *cmd = proc_map.get_proc("flushdb");
			if(!cmd){
				job->link->output->append("flushdb error");
				job->link->flush();
				continue;
			}
			proc_t p = cmd->proc;
			const Request req;
			Response resp;
			int result = (*p)(this, job->link, req, &resp);

			job->resp.push_back("ok");
			break;
		}

		Command *cmd = proc_map.get_proc(req->at(0));
		if(!cmd){
			job->resp.push_back("client_error");
			job->resp.push_back("Unknown Command: " + req->at(0).String());
			break;
		}
		job->cmd = cmd;
		
		if(cmd->flags & Command::FLAG_THREAD){
			if(cmd->flags & Command::FLAG_WRITE){
				writer->push(job);
			}else{
				reader->push(job);
			}
			return PROC_THREAD;
		}

		proc_t p = cmd->proc;
		job->time_wait = 1000 * (millitime() - job->stime);
		job->result = (*p)(this, job->link, *req, &job->resp);
		job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;
	}while(0);



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

	if (job->link->append_reply) {
        if (!job->resp.resp.empty()) {
            if(job->link->send_append_res(job->resp.get_append_array()) == -1){
                log_debug("job->link->send_append_res error");
                job->result = PROC_ERROR;
                return job->result;
            }

        }
    }

	return job->result;
}


/* built-in procs */

static int proc_ping(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	return 0;
}

static int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back("ideawu's network server framework");
	resp->push_back("version");
	resp->push_back("1.0");
	resp->push_back("links");
	resp->add(net->link_count);
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
	return 0;
}

static int proc_auth(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		if(!net->need_auth || req[1] == net->password){
			link->auth = true;
			resp->push_back("ok");
			resp->push_back("1");
		}else{
			reply_errinfo_return("invalid password");
		}
	}
	return 0;
}

