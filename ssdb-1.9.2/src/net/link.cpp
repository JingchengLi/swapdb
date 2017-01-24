/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/un.h>
#include <sys/ioctl.h>

#include "link.h"

#include "link_redis.cpp"
#include "redis/rdb.h"

#define INIT_BUFFER_SIZE  1024
#define BEST_BUFFER_SIZE  (8 * 1024)


Link::Link(bool is_server) {
    redis = NULL;

    sock = -1;
    noblock_ = false;
    error_ = false;
    remote_ip[0] = '\0';
    remote_port = -1;
    auth = false;
    ignore_key_range = false;

    if (is_server) {
        input = output = NULL;
    } else {
        input = new Buffer(INIT_BUFFER_SIZE);
        output = new Buffer(INIT_BUFFER_SIZE);
        input_buffer = new Buffer(INIT_BUFFER_SIZE);
    }
}

Link::~Link() {
    if (redis) {
        delete redis;
    }
    if (input) {
        delete input;
    }
    if (output) {
        delete output;
    }
    if (input_buffer) {
        delete input_buffer;
    }
    this->close();
}

void Link::close() {
    if (sock >= 0) {
        ::close(sock);
    }
}

void Link::nodelay(bool enable) {
    int opt = enable ? 1 : 0;
    ::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *) &opt, sizeof(opt));
}

void Link::keepalive(bool enable) {
    int opt = enable ? 1 : 0;
    ::setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &opt, sizeof(opt));
}

void Link::noblock(bool enable) {
    noblock_ = enable;
    if (enable) {
        ::fcntl(sock, F_SETFL, O_NONBLOCK | O_RDWR);
    } else {
        ::fcntl(sock, F_SETFL, O_RDWR);
    }
}

// TODO: check less than 256
static bool is_ip(const char *host) {
    int dot_count = 0;
    int digit_count = 0;
    for (const char *p = host; *p; p++) {
        if (*p == '.') {
            dot_count += 1;
            if (digit_count >= 1 && digit_count <= 3) {
                digit_count = 0;
            } else {
                return false;
            }
        } else if (*p >= '0' && *p <= '9') {
            digit_count += 1;
        } else {
            return false;
        }
    }
    return dot_count == 3;
}

Link *Link::connect(const char *host, int port) {
    Link *link;
    int sock = -1;

    char ip_resolve[INET_ADDRSTRLEN];
    if (!is_ip(host)) {
        struct hostent *hptr = gethostbyname(host);
        for (int i = 0; hptr && hptr->h_addr_list[i] != NULL; i++) {
            struct in_addr *addr = (struct in_addr *) hptr->h_addr_list[i];
            if (inet_ntop(AF_INET, addr, ip_resolve, sizeof(ip_resolve))) {
                //printf("resolve %s: %s\n", host, ip_resolve);
                host = ip_resolve;
                break;
            }
        }
    }

    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short) port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        goto sock_err;
    }
    if (::connect(sock, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        goto sock_err;
    }

    //log_debug("fd: %d, connect to %s:%d", sock, ip, port);
    link = new Link();
    link->sock = sock;
    link->keepalive(true);
    return link;
    sock_err:
    //log_debug("connect to %s:%d failed: %s", ip, port, strerror(errno));
    if (sock >= 0) {
        ::close(sock);
    }
    return NULL;
}

Link *Link::unixsocket(const std::string &path) {
    unlink(path.c_str());
    Link *link;
    int sock = -1;
    int opt = 1;
    struct sockaddr_un sa;

    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_LOCAL;
    strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);

    if ((sock = ::socket(AF_LOCAL, SOCK_STREAM, 0)) == -1) {
        goto sock_err;
    }
    if (::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        goto sock_err;
    }
    if (::bind(sock, (struct sockaddr *) &sa, sizeof(sa)) == -1) {
        goto sock_err;
    }
    if (::listen(sock, 1024) == -1) {
        goto sock_err;
    }

    link = new Link(true);
    link->sock = sock;
    snprintf(link->remote_ip, sizeof(link->remote_ip), "%s", "");
    link->remote_port = 0;
    return link;

sock_err:
    //log_debug("listen %s:%d failed: %s", ip, port, strerror(errno));
    if (sock >= 0) {
        ::close(sock);
    }
    return NULL;
}

Link *Link::listen(const char *ip, int port) {
    Link *link;
    int sock = -1;

    int opt = 1;
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short) port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        goto sock_err;
    }
    if (::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        goto sock_err;
    }
    if (::bind(sock, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        goto sock_err;
    }
    if (::listen(sock, 1024) == -1) {
        goto sock_err;
    }
    //log_debug("server socket fd: %d, listen on: %s:%d", sock, ip, port);

    link = new Link(true);
    link->sock = sock;
    snprintf(link->remote_ip, sizeof(link->remote_ip), "%s", ip);
    link->remote_port = port;
    return link;
sock_err:
    //log_debug("listen %s:%d failed: %s", ip, port, strerror(errno));
    if (sock >= 0) {
        ::close(sock);
    }
    return NULL;
}

Link *Link::accept() {
    Link *link;
    int client_sock;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);

    while ((client_sock = ::accept(sock, (struct sockaddr *) &addr, &addrlen)) == -1) {
        if (errno != EINTR) {
            //log_error("socket %d accept failed: %s", sock, strerror(errno));
            return NULL;
        }
    }

    // avoid client side TIME_WAIT
    struct linger opt = {1, 0};
    int ret = ::setsockopt(client_sock, SOL_SOCKET, SO_LINGER, (void *) &opt, sizeof(opt));
    if (ret != 0) {
        //log_error("socket %d set linger failed: %s", client_sock, strerror(errno));
    }

    link = new Link();
    link->sock = client_sock;
    link->keepalive(true);
    inet_ntop(AF_INET, &addr.sin_addr, link->remote_ip, sizeof(link->remote_ip));
    link->remote_port = ntohs(addr.sin_port);
    return link;
}

int Link::read() {
    int ret = 0;
    int want;

    input->nice();
    // 由于 recv() 返回的数据是指向 input 所占的内存, 所以, 不能在 recv()
    // 之后立即就释放内存, 只能在下一次read()的时候再释放.
    if (input->size() == 0 && input->total() > BEST_BUFFER_SIZE) {
        input->shrink(BEST_BUFFER_SIZE);
    }

    while ((want = input->space()) > 0) {
        // test
        //want = 1;
        int len = ::read(sock, input->slot(), want);
        if (len == -1) {
            if (errno == EINTR) {
                continue;
            } else if (errno == EWOULDBLOCK) {
                break;
            } else {
                //log_debug("fd: %d, read: -1, want: %d, error: %s", sock, want, strerror(errno));
                return -1;
            }
        } else {
            //log_debug("fd: %d, want=%d, read: %d", sock, want, len);
            if (len == 0) {
                return 0;
            }
            ret += len;
            input->incr(len);
        }
        if (!noblock_) {
            break;
        }
    }
    //log_debug("read %d", ret);
    //printf("%s\n", hexmem(input->data(), input->size()).c_str());
    return ret;
}

int Link::write() {
    int ret = 0;
    int want;
    while ((want = output->size()) > 0) {
        // test
        //want = 1;
        int len = ::write(sock, output->data(), want);
        if (len == -1) {
            if (errno == EINTR) {
                continue;
            } else if (errno == EWOULDBLOCK) {
                break;
            } else {
                //log_debug("fd: %d, write: -1, error: %s", sock, strerror(errno));
                return -1;
            }
        } else {
            //log_debug("fd: %d, want: %d, write: %d", sock, want, len);
            if (len == 0) {
                // ?
                break;
            }
            ret += len;
            output->decr(len);
        }
        if (!noblock_) {
            break;
        }
    }
    output->nice();
    if (output->size() == 0 && output->total() > BEST_BUFFER_SIZE) {
        output->shrink(BEST_BUFFER_SIZE);
    }
    return ret;
}

int Link::flush() {
    int len = 0;
    while (!output->empty()) {
        int ret = this->write();
        if (ret == -1) {
            return -1;
        }
        len += ret;
    }
    return len;
}

const std::vector<Bytes> *Link::recv() {
    this->recv_data.clear();

    if (input->empty()) {
        return &this->recv_data;
    }

    // TODO: 记住上回的解析状态
    int parsed = 0;
    int size = input->size();
    char *head = input->data();

    // ignore leading empty lines
    while (size > 0 && (head[0] == '\n' || head[0] == '\r')) {
        head++;
        size--;
        parsed++;
    }

    // Redis protocol supports
    if (head[0] == '*') {
        if (redis == NULL) {
            redis = new RedisLink();
        }
        const std::vector<Bytes> *ret = redis->recv_req(input);
        if (ret) {
            this->recv_data = *ret;
            return &this->recv_data;
        } else {
            return NULL;
        }
    }

    while (size > 0) {
        char *body = (char *) memchr(head, '\n', size);
        if (body == NULL) {
            break;
        }
        body++;

        int head_len = body - head;
        if (head_len == 1 || (head_len == 2 && head[0] == '\r')) {
            // packet end
            parsed += head_len;
            input->decr(parsed);
            return &this->recv_data;;
        }
        if (head[0] < '0' || head[0] > '9') {
            //log_warn("bad format");
            return NULL;
        }

        char head_str[20];
        if (head_len > (int) sizeof(head_str) - 1) {
            return NULL;
        }
        memcpy(head_str, head, head_len - 1); // no '\n'
        head_str[head_len - 1] = '\0';

        int body_len = atoi(head_str);
        if (body_len < 0) {
            //log_warn("bad format");
            return NULL;
        }
        //log_debug("size: %d, head_len: %d, body_len: %d", size, head_len, body_len);
        size -= head_len + body_len;
        if (size < 0) {
            break;
        }

        this->recv_data.push_back(Bytes(body, body_len));

        head += head_len + body_len;
        parsed += head_len + body_len;
        if (size >= 1 && head[0] == '\n') {
            head += 1;
            size -= 1;
            parsed += 1;
        } else if (size >= 2 && head[0] == '\r' && head[1] == '\n') {
            head += 2;
            size -= 2;
            parsed += 2;
        } else if (size >= 2) {
            // bad format
            return NULL;
        } else {
            break;
        }
        if (parsed > MAX_PACKET_SIZE) {
            //log_warn("fd: %d, exceed max packet size, parsed: %d", this->sock, parsed);
            return NULL;
        }
    }

    if (input->space() == 0) {
        input->nice();
        if (input->space() == 0) {
            if (input->grow() == -1) {
                //log_error("fd: %d, unable to resize input buffer!", this->sock);
                return NULL;
            }
            //log_debug("fd: %d, resize input buffer, %s", this->sock, input->stats().c_str());
        }
    }

    // not ready
    this->recv_data.clear();
    return &this->recv_data;
}

static int ssdb_load_len(const char *data, int *offset, uint64_t *lenptr){
    unsigned char buf[2];
    buf[0] = (unsigned char)data[0];
    buf[1] = (unsigned char)data[1];
    int type;
    type = (buf[0]&0xC0)>>6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        *lenptr = buf[0]&0x3F;
        *offset = 1;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0]&0x3F;
        *offset = 1;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        *lenptr = ((buf[0]&0x3F)<<8)|buf[1];
        *offset = 2;
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        len = *(uint32_t*)(data+1);
        *lenptr = be32toh(len);
        *offset = 1 + sizeof(uint32_t);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        len = *(uint64_t*)(data+1);
        *lenptr = be64toh(len);
        *offset = 1 + sizeof(uint64_t);
    } else {
        printf("Unknown length encoding %d in rdbLoadLen()",type);
        return -1; /* Never reached. */
    }
    return 0;
}

int Link::parse_sync_data() {
    input_buffer->nice();
    if (input_buffer->size() == 0 && input_buffer->total() > BEST_BUFFER_SIZE) {
        input_buffer->shrink(BEST_BUFFER_SIZE);
    }
    if (input_buffer->space() < input->size()) {
        input_buffer->grow();
    }
    if (input_buffer->space() < input->size()) {
        memcpy(input_buffer->slot(), input->data(), input_buffer->space());
        input_buffer->incr(input_buffer->space());
        input->decr(input_buffer->space());
    } else{
        memcpy(input_buffer->slot(), input->data(), input->size());
        input_buffer->incr(input->size());
        input->decr(input->size());
    }

    while (input_buffer->size() > 0){
        int key_offset = 0, val_offset = 0;
        uint64_t key_len = 0, val_len = 0;
        if (ssdb_load_len(input_buffer->data(), &key_offset, &key_len) == -1){
            return -1;
        }
        if (input_buffer->size() < (int)key_len){
            break;
        }
        std::string key;
        key.append(input_buffer->data()+key_offset, key_len);
        input_buffer->decr(key_offset + (int)key_len);

        if (ssdb_load_len(input_buffer->data(), &val_offset, &val_len) == -1){
            return -1;
        }
        if (input_buffer->size() < (int)val_len){
            input_buffer->incr(key_offset + (int)key_len);
            break;
        }
        std::string value;
        value.append(input_buffer->data()+val_offset, val_len);
        input_buffer->decr(val_offset + (int)val_len);
        sync_data.push_back(key);
        sync_data.push_back(value);
    }
    return 0;
}

int Link::send(const std::vector<std::string> &resp) {
    if (resp.empty()) {
        return 0;
    }
    // Redis protocol supports
    if (this->redis) {
        return this->redis->send_resp(this->output, resp);
    }

    for (int i = 0; i < resp.size(); i++) {
        output->append_record(resp[i]);
    }
    output->append('\n');
    return 0;
}

int Link::send(const std::vector<Bytes> &resp) {
    for (int i = 0; i < resp.size(); i++) {
        output->append_record(resp[i]);
    }
    output->append('\n');
    return 0;
}

int Link::send(const Bytes &s1) {
    output->append_record(s1);
    output->append('\n');
    return 0;
}

int Link::send(const Bytes &s1, const Bytes &s2) {
    output->append_record(s1);
    output->append_record(s2);
    output->append('\n');
    return 0;
}

int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3) {
    output->append_record(s1);
    output->append_record(s2);
    output->append_record(s3);
    output->append('\n');
    return 0;
}

int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4) {
    output->append_record(s1);
    output->append_record(s2);
    output->append_record(s3);
    output->append_record(s4);
    output->append('\n');
    return 0;
}

int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5) {
    output->append_record(s1);
    output->append_record(s2);
    output->append_record(s3);
    output->append_record(s4);
    output->append_record(s5);
    output->append('\n');
    return 0;
}

const std::vector<Bytes> *Link::response() {
    while (1) {
        const std::vector<Bytes> *resp = this->recv();
        if (resp == NULL) {
            return NULL;
        } else if (resp->empty()) {
            if (this->read() <= 0) {
                return NULL;
            }
        } else {
            return resp;
        }
    }
    return NULL;
}

const std::vector<Bytes> *Link::request(const Bytes &s1) {
    if (this->send(s1) == -1) {
        return NULL;
    }
    if (this->flush() == -1) {
        return NULL;
    }
    return this->response();
}

const std::vector<Bytes> *Link::request(const Bytes &s1, const Bytes &s2) {
    if (this->send(s1, s2) == -1) {
        return NULL;
    }
    if (this->flush() == -1) {
        return NULL;
    }
    return this->response();
}

const std::vector<Bytes> *Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3) {
    if (this->send(s1, s2, s3) == -1) {
        return NULL;
    }
    if (this->flush() == -1) {
        return NULL;
    }
    return this->response();
}

const std::vector<Bytes> *Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4) {
    if (this->send(s1, s2, s3, s4) == -1) {
        return NULL;
    }
    if (this->flush() == -1) {
        return NULL;
    }
    return this->response();
}

const std::vector<Bytes> *
Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5) {
    if (this->send(s1, s2, s3, s4, s5) == -1) {
        return NULL;
    }
    if (this->flush() == -1) {
        return NULL;
    }
    return this->response();
}


RedisResponse *Link::redisResponse() {
    // Redis protocol supports
    // - + : $
    if (redis == NULL) {
        redis = new RedisLink();
    }

    RedisResponse *resp = new RedisResponse();

    while (1) {
        int parsed = redis->recv_res(input, resp, 0);
        if (resp->status == REDIS_RESPONSE_ERR) {
            input->decr(parsed);
            return resp;
        } else if (resp->status == REDIS_RESPONSE_RETRY) { //retry
            resp->reset();
            if (this->read() <= 0) {
                delete resp;
                return NULL;
            }
        } else {
            input->decr(parsed);
            return resp;
        }
    }

    delete resp;
    return NULL;
}

int Link::redisRequestSend(const std::vector<std::string> &args) {
    std::string tmp;
    tmp.append("*");
    tmp.append(str(args.size()));
    tmp.append("\r\n");
    for (const std::string &s: args) {
        tmp.append("$");
        tmp.append(str(s.size()));
        tmp.append("\r\n");
        tmp.append(s);
        tmp.append("\r\n");
    }
    tmp.append("\r\n");
    return output->append(tmp.data(), tmp.size());
}

RedisResponse *Link::redisRequest(const std::vector<std::string> &args) {
    if (this->redisRequestSend(args) == -1) {
        log_error("redisRequestSend error");
        return NULL;
    }

    if (this->flush() == -1) {
        log_error("redisRequest flush error");
        return NULL;
    }

    return this->redisResponse();
}

#if 0
int main(){
    //Link link;
    //link.listen("127.0.0.1", 8888);
    Link *link = Link::connect("127.0.0.1", 8080);
    printf("%d\n", link);
    getchar();
    return 0;
}
#endif
