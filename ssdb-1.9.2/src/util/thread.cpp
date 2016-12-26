//
// Created by a1 on 16-12-26.
//

#include "thread.h"

RefMutex::RefMutex() {
    refs_ = 0;
    pthread_mutex_init(&mu_, nullptr);
}

RefMutex::~RefMutex() {
    pthread_mutex_destroy(&mu_);
}

void RefMutex::Ref() {
    refs_++;
}
void RefMutex::Unref() {
    --refs_;
    if (refs_ == 0) {
        delete this;
    }
}

void RefMutex::Lock() {
    pthread_mutex_lock(&mu_);
}

void RefMutex::Unlock() {
    pthread_mutex_unlock(&mu_);
}

RecordMutex::~RecordMutex() {
    mutex_.lock();

    std::unordered_map<std::string, RefMutex *>::const_iterator it = records_.begin();
    for (; it != records_.end(); it++) {
        delete it->second;
    }
    mutex_.unlock();
}

int64_t RecordMutex::GetUsage() {
    int64_t size = 0;
    mutex_.lock();
    size = charge_;
    mutex_.unlock();
    return size;
}

const int64_t kEstimatePairSize = sizeof(std::string) + sizeof(RefMutex) + sizeof(std::pair<std::string, void *>);

void RecordMutex::Lock(const std::string &key) {
    mutex_.lock();
    std::unordered_map<std::string, RefMutex *>::const_iterator it = records_.find(key);

    if (it != records_.end()) {
        //log_info ("tid=(%u) >Lock key=(%s) exist, map_size=%u", pthread_self(), key.c_str(), records_.size());
        RefMutex *ref_mutex = it->second;
        ref_mutex->Ref();
        mutex_.unlock();

        ref_mutex->Lock();
        //log_info ("tid=(%u) <Lock key=(%s) exist", pthread_self(), key.c_str());
    } else {
        //log_info ("tid=(%u) >Lock key=(%s) new, map_size=%u ++", pthread_self(), key.c_str(), records_.size());
        RefMutex *ref_mutex = new RefMutex();

        records_.insert(std::make_pair(key, ref_mutex));
        ref_mutex->Ref();
        charge_ += kEstimatePairSize + key.size();
        mutex_.unlock();

        ref_mutex->Lock();
        //log_info ("tid=(%u) <Lock key=(%s) new", pthread_self(), key.c_str());
    }
}

void RecordMutex::Unlock(const std::string &key) {
    mutex_.lock();
    std::unordered_map<std::string, RefMutex *>::const_iterator it = records_.find(key);

    //log_info ("tid=(%u) >Unlock key=(%s) new, map_size=%u --", pthread_self(), key.c_str(), records_.size());
    if (it != records_.end()) {
        RefMutex *ref_mutex = it->second;

        if (ref_mutex->IsLastRef()) {
            charge_ -= kEstimatePairSize + key.size();
            records_.erase(it);
        }
        ref_mutex->Unlock();
        ref_mutex->Unref();
    }

    mutex_.unlock();
    //log_info ("tid=(%u) <Unlock key=(%s) new", pthread_self(), key.c_str());
}
