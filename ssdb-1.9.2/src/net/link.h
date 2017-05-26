/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_LINK_H_
#define NET_LINK_H_

//#define NET_DEBUG ON

#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "../util/bytes.h"

#include "link_redis.h"
class Context;

class Link{
	private:
		int sock;
		bool noblock_;
		bool error_;
		std::vector<Bytes> recv_data;
	public:
		bool append_reply;

		const static int MAX_PACKET_SIZE = 128 * 1024 * 1024;

		char remote_ip[INET_ADDRSTRLEN];
		int remote_port;

		bool auth;
		bool ignore_key_range;

		Buffer *input;
		Buffer *output;

		RedisLink *redis;
		Context *context = nullptr;
		double create_time;
		double active_time;

		Link(bool is_server=false);
		~Link();
		void close();
		void nodelay(bool enable=true);
		// noblock(true) is supposed to corperate with IO Multiplex,
		// otherwise, flush() may cause a lot unneccessary write calls.
		void noblock(bool enable=true);
		void keepalive(bool enable=true);

		int readtimeout(long timeout_ms);
		int sendtimeout(long timeout_ms);

		int fd() const{
			return sock;
		}
		bool error() const{
			return error_;
		}
		void mark_error(){
			error_ = true;
		}

		static Link *unixsocket(const std::string &path);
		static Link* connect(const char *ip, int port, long timeout_ms = -1);
		static Link* listen(const char *ip, int port);
		Link* accept();

		// read network data info buffer
		int read();
		int read(int shrink);
		int write();
		// flush buffered data to network
		// REQUIRES: nonblock
		int flush();

		/**
		 * parse received data, and return -
		 * NULL: error
		 * empty vector: recv not ready
		 * vector<Bytes>: recv ready
		 */
		const std::vector<Bytes>* recv();
		// wait until a response received.
		const std::vector<Bytes>* response();

		// need to call flush to ensure all data has flush into network
		int send(const std::vector<std::string> &packet);
		int send(const std::vector<Bytes> &packet);
		int send(const Bytes &s1);
		int send(const Bytes &s1, const Bytes &s2);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);
		int send_append_res(const std::vector<std::string> &packet);
		int quick_send(const std::vector<std::string> &packet);

		const std::vector<Bytes>* last_recv(){
			return &recv_data;
		}

		/** these methods will send a request to the server, and wait until a response received.
		 * @return
		 * NULL: error
		 * vector<Bytes>: response ready
		 */
		const std::vector<Bytes>* request(const Bytes &s1);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);



};

#ifdef NET_DEBUG
#define net_debug(fmt, args...) log_write(Logger::LEVEL_DEBUG, "%s:%d " fmt, __FILENAME__, __LINE__, ##args)
#else
#define net_debug(fmt, args...) do {} while (0)
#endif

#endif
