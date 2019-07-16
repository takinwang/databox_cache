/*
 * async_writer.hpp
 *
 *  Created on: Dec 1, 2018
 *      Author: root
 */

#ifndef BACKEND_ASYNC_WRITER_HPP_
#define BACKEND_ASYNC_WRITER_HPP_

#include <mutex>
#include <memory>
#include <unordered_map>
#include <vector>
#include <functional>

#include <databox/thrdtimer.hpp>
#include <databox/mtsafe_object.hpp>
#include <databox/lrucache.hpp>

struct AsyncData {
	std::function<void(void)> callback;
};

class AsyncWriter: public std::enable_shared_from_this<AsyncWriter> {
private:
	mtsafe::thread_safe_queue_sync<std::shared_ptr<AsyncData> > * pAsyncDatas { NULL };
	std::string prefix;
	std::shared_ptr<ThreadTimer> oThreadTimer;
public:
	AsyncWriter(mtsafe::thread_safe_queue_sync<std::shared_ptr<AsyncData> > * pAsyncDatas_, const std::string & prefix_);

	void Start();

	void Stop();

	void notify_one();
};

class AsyncWriters {
private:
	int workers_ { 1 };
	bool f_started { false };
	std::string prefix;
	std::mutex mtx;
	bool f_terminated { false };

	mtsafe::thread_safe_queue_sync<std::shared_ptr<AsyncData> > oAsyncDatas;
	std::vector<std::shared_ptr<AsyncWriter> > oAsyncWriters;
public:

	void Start();

	void Stop();

	bool Put(const std::shared_ptr<AsyncData> & oAsyncData, bool nowait);

	void notify_one();

	void setWorkers(int workers) {
		workers_ = std::max<int>(1, workers);
		workers_ = std::min<int>(64, workers_);
	}

	void setPrefix(const std::string& prefix) {
		this->prefix = prefix;
	}
};

inline void AsyncWriters::Start() {
	if (f_started == true) {
		return;
	}
	f_started = true;
	f_terminated = false;
	LOGGER_TRACE("#" << __LINE__ << ", AsyncWriters::Start, " << prefix << ", Backend Workers: " << workers_);

	for (int i = 0; i < workers_; i++) {
		std::shared_ptr<AsyncWriter> aWriter = std::make_shared<AsyncWriter>(&oAsyncDatas, prefix);
		oAsyncWriters.push_back(aWriter);
	}

	for (const std::shared_ptr<AsyncWriter> & aWriter : oAsyncWriters) {
		aWriter->Start();
	}
}

inline void AsyncWriters::Stop() {
	if (f_started == false) {
		return;
	}
	f_started = false;
	f_terminated = true;
	LOGGER_TRACE("#" << __LINE__ << ", AsyncWriters::Stop, " << prefix << ", Backend Workers: " << workers_);

	for (const std::shared_ptr<AsyncWriter> & aWriter : oAsyncWriters) {
		aWriter->Stop();
	}
	oAsyncWriters.clear();
}

inline bool AsyncWriters::Put(const std::shared_ptr<AsyncData>& oAsyncData, bool nowait) {
	if (f_terminated == true) {
		return false;
	}
	size_t nitems = 0;
	if (nowait == false) { //异步模式，按先后顺序执行，置于底部
		nitems = oAsyncDatas.push_back(oAsyncData);
	} else { //同步调用，放在任务顶端
		nitems = oAsyncDatas.push_front(oAsyncData);
	}

	int nms = nitems / 10; //待刷新的数据过多，需要降低 put 速度，以方便 flush 线程持久化写入
	if (nms > 0) {
		//slow down put
		LOGGER_TRACE(
				"#" << __LINE__ << ", AsyncWriters::Put, "<< prefix << ", " << nitems << " items, Slow down " << nms << " ms")
		std::this_thread::sleep_for(std::chrono::milliseconds(nms));
	}

	notify_one();
	return true;
}

inline void AsyncWriters::notify_one() {
	for (auto & aWriter : oAsyncWriters) {
		aWriter->notify_one();
	}
}

inline AsyncWriter::AsyncWriter(mtsafe::thread_safe_queue_sync<std::shared_ptr<AsyncData> > * pAsyncDatas_,
		const std::string & prefix_) :
		pAsyncDatas(pAsyncDatas_), prefix(prefix_) {
}

inline void AsyncWriter::Start() {
	if (oThreadTimer.get() != NULL) {
		return;
	}

	auto self = this->shared_from_this();
	oThreadTimer = std::make_shared<ThreadTimer>([ this, self ] (bool timeout ) {
		if(timeout == true) {
			//等久了，没有数据过来
			return true;
		}

		std::shared_ptr<AsyncData> oAsyncData;
		while( true) {
			if (pAsyncDatas->pop_front(oAsyncData) == false ) { //no more data
//				LOGGER_TRACE("#" << __LINE__ << ", AsyncWriter::Start, "<< prefix << ", No more AsyncData")
				break;
			}

			if( oAsyncData->callback == nullptr ) {
				continue;
			}

			oAsyncData->callback( );
		}

		return true;
	}, "AsyncWriter");

	LOGGER_TRACE("#" << __LINE__ << ", AsyncWriter::Start, "<< prefix << ", Backend AsyncWriter: " << (long) this)

	oThreadTimer->start(2);
}

inline void AsyncWriter::Stop() {
	if (oThreadTimer.get() == NULL) {
		return;
	}
	oThreadTimer->stop();
	oThreadTimer.reset();
}

inline void AsyncWriter::notify_one() {
	if (oThreadTimer.get() == NULL) {
		return;
	}
	oThreadTimer->notify_one();
}

#endif /* BACKEND_ASYNC_WRITER_HPP_ */
