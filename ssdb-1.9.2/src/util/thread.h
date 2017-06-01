/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_THREAD_H_
#define UTIL_THREAD_H_

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <queue>
#include <vector>
#include <string>
#include <unordered_map>
#include <sys/time.h>
#include <atomic>

class Mutex{
	private:
		friend class CondVar;
		pthread_mutex_t mutex;
	public:
		Mutex(){
			pthread_mutex_init(&mutex, NULL);
		}
		~Mutex(){
			pthread_mutex_destroy(&mutex);
		}
		void lock(){
			pthread_mutex_lock(&mutex);
		}
		void unlock(){
			pthread_mutex_unlock(&mutex);
		}
};


class SpinMutexLock {
	std::atomic_flag locked = ATOMIC_FLAG_INIT ;
public:
	void lock() {
		while (locked.test_and_set(std::memory_order_acquire)) {
			sched_yield();
		}
	}
	void unlock() {
		locked.clear(std::memory_order_release);
	}
};


class CondVar{
	private:
		pthread_cond_t	cv_;
		Mutex*			mu_;
	public:
		explicit CondVar(Mutex* mu)
		: mu_(mu) {
			pthread_cond_init(&cv_, NULL);
		}
		~CondVar(){
			pthread_cond_destroy(&cv_);
		}
		void wait(){
			pthread_cond_wait(&cv_, &mu_->mutex);
		}
		void waitFor(int s, int ms){
			struct timeval now;
			struct timespec outtime;
			gettimeofday(&now, NULL);
			outtime.tv_sec = now.tv_sec + s;
			outtime.tv_nsec = now.tv_usec * 1000 + ms;
			pthread_cond_timedwait(&cv_, &mu_->mutex, &outtime);
		}
		void signal(){
			pthread_cond_signal(&cv_);
		}
		void signalAll(){
			pthread_cond_broadcast(&cv_);
		}
};

template <typename T>
class Locking{
	private:
	T *mutex;
		// No copying allowed
		Locking(const Locking&);
		void operator=(const Locking&);
	public:
		Locking(T *mutex){
			this->mutex = mutex;
			this->mutex->lock();
		}
		~Locking(){
			this->mutex->unlock();
		}

};

template <typename T>
class RefMutex {
public:
	RefMutex() {refs_ = 0;}
	~RefMutex() {;}

	// Lock and Unlock will increase and decrease refs_,
	// should check refs before Unlock
	void Lock() {
		mu_.lock();
	}

	void Unlock() {
		mu_.unlock();
	}

	void Ref() {
		refs_++;
	}

	void Unref() {
		--refs_;
		if (refs_ == 0) {
			delete this;
		}
	}

	bool IsLastRef() {
		return refs_ == 1;
	}

private:
	T mu_;
	int refs_;

	// No copying
	RefMutex(const RefMutex&);
	void operator=(const RefMutex&);
};

template <typename T>
class RecordMutex {
public:
	RecordMutex() : charge_(0) {}

	~RecordMutex() {
		mutex_.lock();

		typename std::unordered_map<std::string, RefMutex<T> *>::const_iterator it = records_.begin();
		for (; it != records_.end(); it++) {
			delete it->second;
		}
		mutex_.unlock();
	}

	int64_t GetUsage() {
		int64_t size = 0;
		mutex_.lock();
		size = charge_;
		mutex_.unlock();
		return size;
	}

	const int64_t kEstimatePairSize = sizeof(std::string) + sizeof(RefMutex<T> *    ) + sizeof(std::pair<std::string, void *>);

	void lock() {
        g_mutex_.lock();

        while (GetUsage() > 0) {
            sched_yield();
        }

    }

	void unlock() {
        g_mutex_.unlock();
    }

	void Lock(const std::string &key) {
        g_mutex_.lock();
        g_mutex_.unlock();

		mutex_.lock();
		typename std::unordered_map<std::string, RefMutex<T> *>::const_iterator it = records_.find(key);

		if (it != records_.end()) {
			//log_info ("tid=(%u) >Lock key=(%s) exist, map_size=%u", pthread_self(), key.c_str(), records_.size());
			RefMutex<T> *ref_mutex = it->second;
			ref_mutex->Ref();
			mutex_.unlock();

			ref_mutex->Lock();
			//log_info ("tid=(%u) <Lock key=(%s) exist", pthread_self(), key.c_str());
		} else {
			//log_info ("tid=(%u) >Lock key=(%s) new, map_size=%u ++", pthread_self(), key.c_str(), records_.size());
			RefMutex<T> *ref_mutex = new RefMutex<T>();

			records_.insert(std::make_pair(key, ref_mutex));
			ref_mutex->Ref();
			charge_ += kEstimatePairSize + key.size();
			mutex_.unlock();

			ref_mutex->Lock();
			//log_info ("tid=(%u) <Lock key=(%s) new", pthread_self(), key.c_str());
		}
	}

	void Unlock(const std::string &key) {
		mutex_.lock();
		typename std::unordered_map<std::string, RefMutex<T> *>::const_iterator it = records_.find(key);

		//log_info ("tid=(%u) >Unlock key=(%s) new, map_size=%u --", pthread_self(), key.c_str(), records_.size());
		if (it != records_.end()) {
			RefMutex<T> *ref_mutex = it->second;

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

private:
	T mutex_;
    SpinMutexLock g_mutex_;

	std::unordered_map<std::string, RefMutex<T> *> records_;
	int64_t charge_;

	// No copying
	RecordMutex(const RecordMutex&);
	void operator=(const RecordMutex&);
};

template <typename T>
class RecordLock {
public:
    RecordLock(RecordMutex<T> *mu, const std::string &key, bool lock = true)
            : mu_(mu), key_(key) , lock_(lock) {
		if (lock_) {
			mu_->Lock(key_);
		}
    }
    ~RecordLock() {
		if (lock_) {
			mu_->Unlock(key_);
		}
	}

private:
    RecordMutex<T> *const mu_;
    std::string key_;
	bool lock_ = true;

    // No copying allowed
    RecordLock(const RecordLock&);
    void operator=(const RecordLock&);
};

template <typename T>
class RecordLocks {
public:
	RecordLocks(RecordMutex<T> *mu)
            : mu_(mu) {
    }
    ~RecordLocks() {
		for (int i = 0; i < keys_.size(); ++i) {
			mu_->Unlock(keys_[i]);
		}
	}

	void Lock(const std::string& key_) {
		mu_->Lock(key_);
		keys_.push_back(key_);
	};

private:
    RecordMutex<T> *const mu_;
    std::vector<std::string> keys_;

    // No copying allowed
	RecordLocks(const RecordLocks&);
    void operator=(const RecordLocks&);
};

/*
class Semaphore {
	private:
		pthread_cond_t cond;
		pthread_mutex_t mutex;
	public:
		Semaphore(Mutex* mu){
			pthread_cond_init(&cond, NULL);
			pthread_mutex_init(&mutex, NULL);
		}
		~CondVar(){
			pthread_cond_destroy(&cond);
			pthread_mutex_destroy(&mutex);
		}
		void wait();
		void signal();
};
*/


// Thread safe queue
template <class T>
class Queue{
	private:
		pthread_cond_t cond;
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		Queue();
		~Queue();

		bool empty();
		int size();
		std::queue<T> discard();
		int push(const T item);
		// TODO: with timeout
		int pop(T *data);
};


// Selectable queue, multi writers, single reader
template <class T>
class SelectableQueue{
	private:
		int fds[2];
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		SelectableQueue();
		~SelectableQueue();
		int fd(){
			return fds[0];
		}
		int size();
		// multi writer
		int push(const T item);
		// single reader
		int pop(T *data);
};

template<class W, class JOB>
class WorkerPool{
	public:
		class Worker{
			public:
				Worker(){};
				Worker(const std::string &name);
				virtual ~Worker(){}
				int id;
				virtual void init(){}
				virtual void destroy(){}
				virtual int proc(JOB job) = 0;
			private:
			protected:
				std::string name;
		};
	private:
		std::string name;
		Queue<JOB> jobs;
		SelectableQueue<JOB> results;

		int num_workers;
		std::vector<pthread_t> tids;
		bool started;

		struct run_arg{
			int id;
			WorkerPool *tp;
		};
		static void* _run_worker(void *arg);
	public:
		WorkerPool(const char *name="");
		~WorkerPool();

		int fd(){
			return results.fd();
		}
		
		int start(int num_workers);
		int stop();

		std::queue<JOB> discard();
		int queued();
		int push(JOB job);
		int pop(JOB *job);
};





template <class T>
Queue<T>::Queue(){
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
Queue<T>::~Queue(){
	pthread_cond_destroy(&cond);
	pthread_mutex_destroy(&mutex);
}

template <class T>
bool Queue<T>::empty(){
	bool ret = false;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.empty();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
int Queue<T>::size(){
	int ret = -1;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.size();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
std::queue<T> Queue<T>::discard(){
	std::queue<T> empty;
	if(pthread_mutex_lock(&mutex) != 0){
		return empty;
	}
	std::swap(items, empty);

	pthread_mutex_unlock(&mutex);
	return empty;
}

template <class T>
int Queue<T>::push(const T item){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		items.push(item);
	}
	pthread_mutex_unlock(&mutex);
	pthread_cond_signal(&cond);
	return 1;
}

template <class T>
int Queue<T>::pop(T *data){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		// 必须放在循环中, 因为 pthread_cond_wait 可能抢不到锁而被其它处理了
		while(items.empty()){
			//fprintf(stderr, "%d wait\n", pthread_self());
			if(pthread_cond_wait(&cond, &mutex) != 0){
				//fprintf(stderr, "%s %d -1!\n", __FILE__, __LINE__);
				return -1;
			}
			//fprintf(stderr, "%d wait 2\n", pthread_self());
		}
		*data = items.front();
		//fprintf(stderr, "%d job: %d\n", pthread_self(), (int)*data);
		items.pop();
	}
	if(pthread_mutex_unlock(&mutex) != 0){
		//fprintf(stderr, "error!\n");
		return -1;
	}
		//fprintf(stderr, "%d wait end 2, job: %d\n", pthread_self(), (int)*data);
	return 1;
}


template <class T>
SelectableQueue<T>::SelectableQueue(){
	if(pipe(fds) == -1){
		fprintf(stderr, "create pipe error\n");
		exit(0);
	}
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
SelectableQueue<T>::~SelectableQueue(){
	pthread_mutex_destroy(&mutex);
	close(fds[0]);
	close(fds[1]);
}

template <class T>
int SelectableQueue<T>::push(const T item){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	{
		items.push(item);
	}
	if(::write(fds[1], "1", 1) == -1){
		fprintf(stderr, "write fds error\n");
		exit(0);
	}
	pthread_mutex_unlock(&mutex);
	return 1;
}

template <class T>
int SelectableQueue<T>::size(){
	int ret = 0;
	pthread_mutex_lock(&mutex);
	ret = items.size();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
int SelectableQueue<T>::pop(T *data){
	int n, ret = 1;
	char buf[1];

	while(1){
		n = ::read(fds[0], buf, 1);
		if(n < 0){
			if(errno == EINTR){
				continue;
			}else{
				return -1;
			}
		}else if(n == 0){
			ret = -1;
		}else{
			if(pthread_mutex_lock(&mutex) != 0){
				return -1;
			}
			{
				if(items.empty()){
					fprintf(stderr, "%s %d error!\n", __FILE__, __LINE__);
					pthread_mutex_unlock(&mutex);
					return -1;
				}
				*data = items.front();
				items.pop();
			}
			pthread_mutex_unlock(&mutex);
		}
		break;
	}
	return ret;
}



template<class W, class JOB>
WorkerPool<W, JOB>::WorkerPool(const char *name){
	this->name = name;
	this->started = false;
}

template<class W, class JOB>
WorkerPool<W, JOB>::~WorkerPool(){
	if(started){
		stop();
	}
}

template<class W, class JOB>
int WorkerPool<W, JOB>::push(JOB job){
	return this->jobs.push(job);
}

template<class W, class JOB>
int WorkerPool<W, JOB>::queued(){
	return this->jobs.size();
}

template<class W, class JOB>
std::queue<JOB>  WorkerPool<W, JOB>::discard() {
	return this->jobs.discard();;
}


template<class W, class JOB>
int WorkerPool<W, JOB>::pop(JOB *job){
	return this->results.pop(job);
}

template<class W, class JOB>
void* WorkerPool<W, JOB>::_run_worker(void *arg){
	struct run_arg *p = (struct run_arg*)arg;
	int id = p->id;
	WorkerPool *tp = p->tp;
	delete p;

	W w(tp->name);
	Worker *worker = (Worker *)&w;
	worker->id = id;
	worker->init();
	while(1){
		JOB job;
		if(tp->jobs.pop(&job) == -1){
			fprintf(stderr, "jobs.pop error\n");
			::exit(0);
			break;
		}
		worker->proc(job);
		if(tp->results.push(job) == -1){
			fprintf(stderr, "results.push error\n");
			::exit(0);
			break;
		}
	}
	worker->destroy();
	return (void *)NULL;
}

template<class W, class JOB>
int WorkerPool<W, JOB>::start(int num_workers){
	this->num_workers = num_workers;
	if(started){
		return 0;
	}
	int err;
	pthread_t tid;
	for(int i=0; i<num_workers; i++){
		struct run_arg *arg = new run_arg();
		arg->id = i;
		arg->tp = this;

		err = pthread_create(&tid, NULL, &WorkerPool::_run_worker, arg);
		if(err != 0){
			fprintf(stderr, "can't create thread: %s\n", strerror(err));
		}else{
			tids.push_back(tid);
		}
	}
	started = true;
	return 0;
}

template<class W, class JOB>
int WorkerPool<W, JOB>::stop(){
	// TODO: notify works quit and wait
	for(int i=0; i<tids.size(); i++){
		pthread_cancel(tids[i]);
	}
	started = false;
	return 0;
}

#if 0
class MyWorker : public WorkerPool<MyWorker, int>::Worker{
	public:
		int proc(int *job){
			*job = (id + 1) * 100000 + *job;
			return 0;
		}
};

int main(){
	int num_jobs = 1000;
	WorkerPool<MyWorker, int> tp(10);
	tp.start();
	for(int i=0; i<num_jobs; i++){
		//usleep(200 * 1000);
		//printf("job: %d\n", i);
		tp.push_job(i);
	}
	printf("add end\n");
	for(int i=0; i<num_jobs; i++){
		int job;
		tp.pop_result(&job);
		printf("result: %d, %d\n", i, job);
	}
	printf("end\n");
	//tp.stop();
	return 0;
}
#endif

#endif


