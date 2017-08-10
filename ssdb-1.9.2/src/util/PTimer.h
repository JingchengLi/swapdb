/*
Copyright (c) 2017, Timothy. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_PTIMER_H
#define SSDB_PTIMER_H

#include "log.h"
#include <string>
#include <sys/time.h>

class PTimer {
private:

    double threshold;
    timeval t_start;
    timeval t_end;
    std::string name;
public:
    PTimer(std::string name) {
        this->name = name;
        this->threshold = 0;
    }

    PTimer(std::string name, double threshold_second) {
        this->name = name;
        this->threshold = threshold_second;
    }

    void begin() {
        gettimeofday(&t_start, NULL);
    }

    void end(std::string tag = "") {
        gettimeofday(&t_end, NULL);
        double delta = (double) ((t_end.tv_sec - t_start.tv_sec) * 1000000 + (t_end.tv_usec - t_start.tv_usec)) /
                       1000000.0;                                       //second
        if (delta > threshold) {
            log_info("[PTimer] %s costs: %f second %s", name.c_str(), delta, tag.c_str());
        }

     }
};

#ifdef PTIMER
#define PTS(name) PTimer name(#name);name.begin();
#define PTST(name,second) PTimer name(#name,second);name.begin();
#define PTE(name, tag) name.end(tag);
#else
#define PTS(name) ;
#define PTST(name,second) ;
#define PTE(name, tag) ;
#endif

// -D PTIMER=ON
#endif //SSDB_PTIMER_H
