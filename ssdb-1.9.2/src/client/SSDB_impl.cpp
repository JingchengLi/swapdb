#include "SSDB_impl.h"
#include "util/strings.h"
#include <signal.h>
#include <iostream>
using namespace std;

namespace ssdb{

inline static
Status _read_list(const std::vector<std::string> *resp, std::vector<std::string> *ret){
	Status s(resp);
	if(s.ok()){
		std::vector<std::string>::const_iterator it;
		for(it = resp->begin() + 1; it != resp->end(); it++){
			ret->push_back(*it);
		}
	}
	return s;
}

inline static
Status _read_int64(const std::vector<std::string> *resp, int64_t *ret){
	Status s(resp);
	if(s.ok()){
		if(resp->size() >= 2){
			if(ret){
				*ret = str_to_int64(resp->at(1));
			}
		}else{
			return Status("server_error");
		}
	}
	return s;
}

inline static
Status _read_double(const std::vector<std::string> *resp, double *ret){
	Status s(resp);
	if(s.ok()){
		if(resp->size() >= 2){
			if(ret){
				*ret = str_to_double(resp->at(1).data(), resp->at(1).size());
			}
		}else{
			return Status("server_error");
		}
	}
	return s;
}

inline static
Status _read_str(const std::vector<std::string> *resp, std::string *ret){
	Status s(resp);
	if(s.ok()){
		if(resp->size() >= 2){
			*ret = resp->at(1);
		}else{
			return Status("server_error");
		}
	}
	return s;
}

ClientImpl::ClientImpl(){
	link = NULL;
}

ClientImpl::~ClientImpl(){
	if(link){
		delete link;
	}
}

Client* Client::connect(const char *ip, int port){
	return Client::connect(std::string(ip), port);
}

Client* Client::connect(const std::string &ip, int port){
	static bool inited = false;
	if(!inited){
		inited = true;
		signal(SIGPIPE, SIG_IGN);
	}
	ClientImpl *client = new ClientImpl();
	client->link = Link::connect(ip.c_str(), port);
	if(client->link == NULL){
		delete client;
		return NULL;
	}
	return client;
}

const std::vector<std::string>* ClientImpl::request(const std::vector<std::string> &req){
	if(link->send(req) == -1){
		return NULL;
	}
	if(link->flush() == -1){
		return NULL;
	}
	const std::vector<Bytes> *packet = link->response();
	if(packet == NULL){
		return NULL;
	}
	resp_.clear();
	for(std::vector<Bytes>::const_iterator it=packet->begin(); it!=packet->end(); it++){
		const Bytes &b = *it;
		resp_.push_back(b.String());
	}
	return &resp_;
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd){
	std::vector<std::string> req;
	req.push_back(cmd);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2, const std::string &s3){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	req.push_back(s3);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2, const std::string &s3, const std::string &s4){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	req.push_back(s3);
	req.push_back(s4);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2, const std::string &s3, const std::string &s4, const std::string &s5){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	req.push_back(s3);
	req.push_back(s4);
	req.push_back(s5);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2, const std::string &s3, const std::string &s4, const std::string &s5, const std::string &s6){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	req.push_back(s3);
	req.push_back(s4);
	req.push_back(s5);
	req.push_back(s6);
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::vector<std::string> &s2){
	std::vector<std::string> req;
	req.push_back(cmd);
	for(std::vector<std::string>::const_iterator it = s2.begin(); it != s2.end(); ++it){
		req.push_back(*it);
	}
	return request(req);
}

const std::vector<std::string>* ClientImpl::request(const std::string &cmd, const std::string &s2, const std::vector<std::string> &s3){
	std::vector<std::string> req;
	req.push_back(cmd);
	req.push_back(s2);
	for(std::vector<std::string>::const_iterator it = s3.begin(); it != s3.end(); ++it){
		req.push_back(*it);
	}
	return request(req);
}

const std::string ClientImpl::response() {
	int ret = link->read();
	if(ret <= 0){
		return "";
	}

    const std::vector<Bytes> *resp = link->recv();
    std::vector<Bytes>::const_iterator it = resp->begin();
	std::string result(it->data(), it->size());
	return result;
}

/******************** misc *************************/
Status ClientImpl::dump(const std::string &key, std::string *val){
	const std::vector<std::string> *resp;
	resp = this->request("dump", key);
	return _read_str(resp, val);
}

Status ClientImpl::restore(const std::string &key, int64_t ttl, const std::string &data, const std::string &replace, std::string *val){
	std::string s_ttl = str(ttl);
	const std::vector<std::string> *resp;
    if(replace.empty())
        resp = this->request("restore", key, s_ttl, data);
    else
        resp = this->request("restore", key, s_ttl, data, replace);
	return _read_str(resp, val);
}

Status ClientImpl::replic(const std::string &ip, int port) {
	const std::vector<std::string> *resp;
	std::string str_port = str(port);
	resp = this->request("replic", ip, str_port);
	Status s(resp);
	return s;
}

Status ClientImpl::replic(const std::vector<std::string> &items) {
	const std::vector<std::string> *resp;
	resp = this->request("replic", items);
	Status s(resp);
	return s;
}

Status ClientImpl::flushdb() {
	const std::vector<std::string> *resp;
	resp = this->request("flushdb");
	Status s(resp);
	return s;
}

Status ClientImpl::del_snapshot() {
    const std::vector<std::string> *resp;
    resp = this->request("rr_del_snapshot");
    Status s(resp);
    return s;
}

Status ClientImpl::dbsize(int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("dbsize");
	return _read_int64(resp, ret);
}

Status ClientImpl::get_kv_range(std::string *start, std::string *end){
	const std::vector<std::string> *resp;
	resp = this->request("get_kv_range");
	Status s(resp);
	if(s.ok()){
		*start = resp->at(1);
		*end = resp->at(2);
	}
	return s;
}

Status ClientImpl::set_kv_range(const std::string &start, const std::string &end){
	const std::vector<std::string> *resp;
	resp = this->request("set_kv_range", start, end);
	Status s(resp);
	return s;
}

Status ClientImpl::ttl(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("ttl", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::pttl(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("pttl", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::expire(const std::string &key, int64_t ttl, int64_t *ret){
	std::string s_ttl = str(ttl);
	const std::vector<std::string> *resp;
	resp = this->request("expire", key, s_ttl);
	return _read_int64(resp, ret);
}

Status ClientImpl::pexpire(const std::string &key, int64_t ttl, int64_t *ret){
	std::string s_ttl = str(ttl);
	const std::vector<std::string> *resp;
	resp = this->request("pexpire", key, s_ttl);
	return _read_int64(resp, ret);
}
/******************** KV *************************/

Status ClientImpl::get(const std::string &key, std::string *val){
	const std::vector<std::string> *resp;
	resp = this->request("get", key);
	return _read_str(resp, val);
}

Status ClientImpl::set(const std::string &key, const std::string &val){
	const std::vector<std::string> *resp;
	resp = this->request("set", key, val);
	Status s(resp);
	return s;
}

Status ClientImpl::setnx(const std::string &key, const std::string &val, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("setnx", key, val);
    return _read_int64(resp, ret);
}

Status ClientImpl::setbit(const std::string &key, int64_t bitoffset, int on){
	const std::vector<std::string> *resp;
	resp = this->request("setbit", key, str(bitoffset), str(on));
	Status s(resp);
	return s;
}

Status ClientImpl::getbit(const std::string &key, int64_t bitoffset, int64_t* ret){
	const std::vector<std::string> *resp;
	resp = this->request("getbit", key, str(bitoffset));
	return _read_int64(resp, ret);
}

Status ClientImpl::getset(const std::string &key, const std::string &val, std::string *getVal){
	const std::vector<std::string> *resp;
	resp = this->request("getset", key, val);
	return _read_str(resp, getVal);
}

Status ClientImpl::setx(const std::string &key, const std::string &val, int64_t ttl){
	const std::vector<std::string> *resp;
	resp = this->request("setx", key, val, str(ttl));
	Status s(resp);
	return s;
}

Status ClientImpl::psetx(const std::string &key, const std::string &val, int64_t ttl){
	const std::vector<std::string> *resp;
	resp = this->request("psetx", key, val, str(ttl));
	Status s(resp);
	return s;
}

Status ClientImpl::del(const std::string &key){
	const std::vector<std::string> *resp;
	resp = this->request("del", key);
	Status s(resp);
	return s;
}

Status ClientImpl::incr(const std::string &key, int64_t incrby, int64_t *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("incr", key, s_incrby);
	return _read_int64(resp, ret);
}

Status ClientImpl::decr(const std::string &key, int64_t incrby, int64_t *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("decr", key, s_incrby);
	return _read_int64(resp, ret);
}

Status ClientImpl::keys(const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("keys", key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::scan(const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("scan", key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::rscan(const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("rscan", key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_get(const std::vector<std::string> &keys, std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("multi_get", keys);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_set(const std::map<std::string, std::string> &kvs){
	const std::vector<std::string> *resp;
	std::vector<std::string> list;
	for(std::map<std::string, std::string>::const_iterator it = kvs.begin();
		it != kvs.end(); ++it)
	{
		list.push_back(it->first);
		list.push_back(it->second);
	}
	resp = this->request("multi_set", list);
	Status s(resp);
	return s;
}

//update to vector from map for test mset same keys
Status ClientImpl::multi_set(const std::vector<std::string> &kvs){
	const std::vector<std::string> *resp;
	resp = this->request("multi_set", kvs);
	Status s(resp);
	return s;
}

Status ClientImpl::multi_del(const std::vector<std::string> &keys, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("multi_del", keys);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::multi_del(const std::string key, int64_t *ret_size){
	const std::vector<std::string> *resp;
	std::vector<std::string> keys;
	keys.push_back(key);
	resp = this->request("multi_del", keys);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}


/******************** hash *************************/


Status ClientImpl::hget(const std::string &name, const std::string &key, std::string *val){
	const std::vector<std::string> *resp;
	resp = this->request("hget", name, key);
	return _read_str(resp, val);
}

Status ClientImpl::hset(const std::string &name, const std::string &key, const std::string &val){
	const std::vector<std::string> *resp;
	resp = this->request("hset", name, key, val);
	Status s(resp);
	return s;
}

Status ClientImpl::hdel(const std::string &name, const std::string &key){
	const std::vector<std::string> *resp;
	resp = this->request("hdel", name, key);
	Status s(resp);
	return s;
}

Status ClientImpl::hincr(const std::string &name, const std::string &key, int64_t incrby, int64_t *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("hincr", name, key, s_incrby);
	return _read_int64(resp, ret);
}

Status ClientImpl::hdecr(const std::string &name, const std::string &key, int64_t incrby, int64_t *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("hdecr", name, key, s_incrby);
	return _read_int64(resp, ret);
}

Status ClientImpl::hsize(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("hsize", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::hclear(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("hclear", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::hkeys(const std::string &name,
	const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("hkeys", name, key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::hvals(const std::string &name,
	const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("hvals", name, key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::hgetall(const std::string &name,
	 std::vector<std::string> *ret)
{
	const std::vector<std::string> *resp;
	resp = this->request("hgetall", name );
	return _read_list(resp, ret);
}

Status ClientImpl::hscan(const std::string &name,
	const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("hscan", name, key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::hrscan(const std::string &name,
	const std::string &key_start, const std::string &key_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("hrscan", name, key_start, key_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_hget(const std::string &name, const std::vector<std::string> &keys,
	std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("hmget", name, keys);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_hset(const std::string &name, const std::map<std::string, std::string> &kvs){
	const std::vector<std::string> *resp;
	std::vector<std::string> list;
	for(std::map<std::string, std::string>::const_iterator it = kvs.begin();
		it != kvs.end(); ++it)
	{
		list.push_back(it->first);
		list.push_back(it->second);
	}
	resp = this->request("hmset", name, list);
	Status s(resp);
	return s;
}

Status ClientImpl::multi_hset(const std::string &name, const std::vector<std::string> &kvs){
	const std::vector<std::string> *resp;
	resp = this->request("hmset", name, kvs);
	Status s(resp);
	return s;
}

Status ClientImpl::multi_hdel(const std::string &name, const std::vector<std::string> &keys, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("hdel", name, keys);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::multi_hdel(const std::string &name, const std::string &key, int64_t *ret_size){
	const std::vector<std::string> *resp;
	std::vector<std::string> keys;
	keys.push_back(key);
	resp = this->request("hdel", name, keys);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::hexists(const std::string &name, const std::string &key, int64_t *ret) {
    const std::vector<std::string> *resp;
    resp = this->request("hexists", name, key);
    return _read_int64(resp, ret);
}

/******************** set *************************/
Status ClientImpl::sadd(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("sadd", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::sadd(const std::string &name, const std::string &item, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("sadd", name, item);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::srem(const std::string &name, const std::string &item, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("srem", name, item);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::srem(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("srem", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::scard(const std::string &name, int64_t *ret) {
    const std::vector<std::string> *resp;
    resp = this->request("scard", name);
    return _read_int64(resp, ret);
}

Status ClientImpl::sismember(const std::string &name, const std::string &key, int64_t *ret) {
    const std::vector<std::string> *resp;
    resp = this->request("sismember", name, key);
    return _read_int64(resp, ret);
}

Status ClientImpl::smembers(const std::string &name, std::vector<std::string> *ret) {
	const std::vector<std::string> *resp;
	resp = this->request("smembers", name);
	return _read_list(resp, ret);
}

Status ClientImpl::sunion(const std::vector<std::string> &names, std::vector<std::string> *ret) {
	const std::vector<std::string> *resp;
	resp = this->request("sunion", names);
	return _read_list(resp, ret);
}

Status ClientImpl::sunionstore(const std::string &name, const std::vector<std::string> &keys, int64_t *ret) {
	const std::vector<std::string> *resp;
	resp = this->request("sunionstore", name, keys);
	return _read_int64(resp, ret);
}

/******************** zset *************************/



Status ClientImpl::zget(const std::string &name, const std::string &key, double *ret){
	const std::vector<std::string> *resp;
	resp = this->request("zget", name, key);
	return _read_double(resp, ret);
}

Status ClientImpl::zset(const std::string &name, const std::map<std::string, double> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	std::vector<std::string> list;
	for(std::map<std::string, double>::const_iterator it = items.begin();
		it != items.end(); ++it)
	{
		list.push_back(it->first);
		list.push_back(str(it->second));
	}
	resp = this->request("multi_zset", name, list);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::zset(const std::string &name, const std::string &key, double score){
	std::string s_score = str(score);
	const std::vector<std::string> *resp;
	resp = this->request("multi_zset", name, key, s_score);
	Status s(resp);
	return s;
}

Status ClientImpl::zdel(const std::string &name, const std::string &key, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("multi_zdel", name, key);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::zdel(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("multi_zdel", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::zincr(const std::string &name, const std::string &key, double incrby, double *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("zincr", name, key, s_incrby);
	return _read_double(resp, ret);
}

Status ClientImpl::zdecr(const std::string &name, const std::string &key, double incrby, double *ret){
	std::string s_incrby = str(incrby);
	const std::vector<std::string> *resp;
	resp = this->request("zdecr", name, key, s_incrby);
	return _read_double(resp, ret);
}

Status ClientImpl::zsize(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("zsize", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::zclear(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("zclear", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::zrank(const std::string &name, const std::string &key, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("zrank", name, key);
	return _read_int64(resp, ret);
}

Status ClientImpl::zrrank(const std::string &name, const std::string &key, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("zrrank", name, key);
	return _read_int64(resp, ret);
}

//offset is start, limit is stop, same as redis API.
Status ClientImpl::zrange(const std::string &name,
		int64_t offset, int64_t limit,
		std::vector<std::string> *ret)
{
	std::string s_offset = str(offset);
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("zrange", name, s_offset, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::zrrange(const std::string &name,
		int64_t offset, int64_t limit,
		std::vector<std::string> *ret)
{
	std::string s_offset = str(offset);
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("zrrange", name, s_offset, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::zkeys(const std::string &name, const std::string &key_start,
	double *score_start, double *score_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_score_start = score_start? str(*score_start) : "";
	std::string s_score_end = score_end? str(*score_end) : "";
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("zkeys", name, key_start, s_score_start, s_score_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::zscan(const std::string &name, const std::string &key_start,
	int64_t *score_start, int64_t *score_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_score_start = score_start? str(*score_start) : "";
	std::string s_score_end = score_end? str(*score_end) : "";
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("zscan", name, key_start, s_score_start, s_score_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::zrscan(const std::string &name, const std::string &key_start,
	int64_t *score_start, int64_t *score_end,
	uint64_t limit, std::vector<std::string> *ret)
{
	std::string s_score_start = score_start? str(*score_start) : "";
	std::string s_score_end = score_end? str(*score_end) : "";
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("zrscan", name, key_start, s_score_start, s_score_end, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_zget(const std::string &name, const std::vector<std::string> &keys,
	std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("multi_zget", name, keys);
	return _read_list(resp, ret);
}

Status ClientImpl::multi_zset(const std::string &name, const std::map<std::string, int64_t> &kss){
	const std::vector<std::string> *resp;
	std::vector<std::string> s_kss;
	for(std::map<std::string, int64_t>::const_iterator it = kss.begin();
		it != kss.end(); ++it)
	{
		s_kss.push_back(it->first);
		s_kss.push_back(str(it->second));
	}
	resp = this->request("multi_zset", name, s_kss);
	Status s(resp);
	return s;
}

Status ClientImpl::multi_zdel(const std::string &name, const std::vector<std::string> &keys){
	const std::vector<std::string> *resp;
	resp = this->request("multi_zdel", name, keys);
	Status s(resp);
	return s;
}

Status ClientImpl::qpush(const std::string &name, const std::string &item, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush", name, item);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpush(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpush_front(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush_front", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpush_front(const std::string &name, const std::string &item, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush_front", name, item);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpush_back(const std::string &name, const std::vector<std::string> &items, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush_back", name, items);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpush_back(const std::string &name, const std::string &item, int64_t *ret_size){
	const std::vector<std::string> *resp;
	resp = this->request("qpush_back", name, item);
	Status s(resp);
	if(ret_size != NULL && s.ok()){
		if(resp->size() > 1){
			*ret_size = str_to_int64(resp->at(1));
		}else{
			return Status("error");
		}
	}
	return s;
}

Status ClientImpl::qpop(const std::string &name, std::string *item){
	const std::vector<std::string> *resp;
	resp = this->request("qpop", name);
	return _read_str(resp, item);
}

Status ClientImpl::qpop(const std::string &name, int64_t limit, std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("qpop", name, str(limit));
	return _read_list(resp, ret);
}

Status ClientImpl::qpop_front(const std::string &name, std::string *item){
	const std::vector<std::string> *resp;
	resp = this->request("qpop_front", name);
	return _read_str(resp, item);
}

Status ClientImpl::qpop_front(const std::string &name, int64_t limit, std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("qpop_front", name, str(limit));
	return _read_list(resp, ret);
}

Status ClientImpl::qpop_back(const std::string &name, int64_t limit, std::vector<std::string> *ret){
	const std::vector<std::string> *resp;
	resp = this->request("qpop_back", name, str(limit));
	return _read_list(resp, ret);
}

Status ClientImpl::qpop_back(const std::string &name, std::string *item){
	const std::vector<std::string> *resp;
	resp = this->request("qpop_back", name);
	return _read_str(resp, item);
}

Status ClientImpl::qslice(const std::string &name,
		int64_t begin, int64_t end,
		std::vector<std::string> *ret)
{
	std::string s_begin = str(begin);
	std::string s_end = str(end);
	const std::vector<std::string> *resp;
	resp = this->request("qslice", name, s_begin, s_end);
	return _read_list(resp, ret);
}

Status ClientImpl::qrange(const std::string &name, int64_t begin, int64_t limit, std::vector<std::string> *ret){
	std::string s_begin = str(begin);
	std::string s_limit = str(limit);
	const std::vector<std::string> *resp;
	resp = this->request("qrange", name, s_begin, s_limit);
	return _read_list(resp, ret);
}

Status ClientImpl::qclear(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("qclear", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::qsize(const std::string &name, int64_t *ret){
	const std::vector<std::string> *resp;
	resp = this->request("qsize", name);
	return _read_int64(resp, ret);
}

Status ClientImpl::qset(const std::string &name, int64_t index, std::string &val){
	std::string s_index = str(index);
	const std::vector<std::string> *resp;
	resp = this->request("qset", name, s_index, val);
	Status s(resp);
	return s;
}

Status ClientImpl::qget(const std::string &name, int64_t index, std::string *val){
	std::string s_index = str(index);
	const std::vector<std::string> *resp;
	resp = this->request("qget", name, s_index);
	return _read_str(resp, val);
}

}; // namespace ssdb
