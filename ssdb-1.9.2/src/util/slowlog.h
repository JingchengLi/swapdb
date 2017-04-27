//
// Created by zts on 17-4-13.
//

#ifndef SSDB_SLOWLOG_H
#define SSDB_SLOWLOG_H

#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

#include <cstdint>
#include <vector>
#include <ctime>
#include <util/bytes.h>

class SlowlogEntry {

public:
    SlowlogEntry(uint64_t id, const std::vector<Bytes> *req_b, int64_t duration) : id(id), duration(duration) {
        set_req(req_b);
        ts = time(NULL);

    }

    std::vector<std::string> req;
    uint64_t id;       /* Unique entry identifier. */
    int64_t duration; /* Time spent by the query, in nanoseconds. */
    time_t ts;        /* Unix time at which the query was executed. */

    bool operator<(const SlowlogEntry &m) const {
        return duration < m.duration;
    }

    bool operator>(const SlowlogEntry &m) const {
        return duration > m.duration;
    }

    std::string String() const {
        std::string res;

        res.append(std::to_string(id));
        res.append(" ");
        res.append(std::to_string(ts));
        res.append(" ");
        res.append(std::to_string(duration));
        res.append(" ");
        for (std::vector<std::string>::const_iterator it = req.begin(); it != req.end(); it++) {
            res.append(hexstr(*it));
            res.append(" ");
        }

        return res;
    }

    void set_req(const std::vector<Bytes> *req_b) {
        size_t argc = req_b->size();
        size_t slargc = (argc > SLOWLOG_ENTRY_MAX_ARGC) ? SLOWLOG_ENTRY_MAX_ARGC : argc;


        for (size_t j = 0; j < slargc; ++j) {
            if (slargc != argc && j == slargc - 1) {
                req.push_back("... (" + std::to_string(argc - slargc + 1) + " more arguments)");
            }

            const Bytes &r = req_b->at(j);
            if (r.size() > SLOWLOG_ENTRY_MAX_STRING) {
                std::string tmp = std::string(r.data(), SLOWLOG_ENTRY_MAX_STRING);
                req.push_back(tmp);
                req.push_back("... (" + std::to_string(r.size() - SLOWLOG_ENTRY_MAX_STRING) + " more bytes)");
            } else {
                req.push_back(r.String());
            }
        }

    }


};

class Slowlog {
public:

    std::vector<SlowlogEntry> history;

    uint64_t slowlog_entry_max_len = 50;
    uint64_t slowlog_log_slower_than = 10; //ms

    uint64_t slowlog_entry_id = 0;
    int64_t slowlog_min_duration = 0;

    void reset() {
        //clear
        slowlog_entry_id = 0;
        slowlog_min_duration = 0;
        history.clear();
    }

    uint64_t len() {
        //length
        return length;
    }


    void pushEntryIfNeeded(const std::vector<Bytes> *req, int64_t duration) {

        bool full = (history.size() == slowlog_entry_max_len);

        if (duration < slowlog_log_slower_than) {
            return;
        } else if (slowlog_entry_max_len == 0) {
            return;
        }

        if (full) {
            if (duration > slowlog_min_duration) {
                //remove first
                history.erase(history.begin());
            } else {
                return;
            }
        }

        history.emplace_back(SlowlogEntry(slowlog_entry_id++, req, duration));

        sort(history.begin(), history.end());
        slowlog_min_duration = history.front().duration;

    }

private:
    uint64_t length = 10;


};


#endif //SSDB_SLOWLOG_H
