#ifndef NEMO_INCLUDE_NEMO_MUTEX_
#define NEMO_INCLUDE_NEMO_MUTEX_

#include "port.h"

#include <pthread.h>

namespace nemo {

class MutexLock {
 public:
  explicit MutexLock(pthread_mutex_t *mu)
      : mu_(mu)  {
        pthread_mutex_lock(this->mu_);
      }
  ~MutexLock() { pthread_mutex_unlock(this->mu_); }

 private:
  pthread_mutex_t *const mu_;
  // No copying allowed
  MutexLock(const MutexLock&);
  void operator=(const MutexLock&);
};

class RWLock {
 public:
  explicit RWLock(pthread_rwlock_t *mu, bool is_rwlock)
      : mu_(mu)  {
        if (is_rwlock) {
          pthread_rwlock_wrlock(this->mu_);
        } else {
          pthread_rwlock_rdlock(this->mu_);
        }
      }
  ~RWLock() { pthread_rwlock_unlock(this->mu_); }

 private:
  pthread_rwlock_t *const mu_;
  // No copying allowed
  RWLock(const RWLock&);
  void operator=(const RWLock&);
};

class RecordLock {
 public:
  RecordLock(port::RecordMutex *mu, const std::string &key)
      : mu_(mu), key_(key) {
        mu_->Lock(key_);
      }
  ~RecordLock() { mu_->Unlock(key_); }

 private:
  port::RecordMutex *const mu_;
  std::string key_;

  // No copying allowed
  RecordLock(const RecordLock&);
  void operator=(const RecordLock&);
};

}
#endif
