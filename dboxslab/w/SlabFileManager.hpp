/*
 * SlabFileManager.hpp
 *
 *  Created on: Oct 23, 2018
 *      Author: root
 */

#ifndef SLABFILEMANAGER_HPP_
#define SLABFILEMANAGER_HPP_

#include <atomic>
#include <thread>
#include <memory>
#include <set>
#include <list>

#include <databox/filesystemutils.hpp>
#include <databox/mtsafe_object.hpp>
#include <databox/async_messager.hpp>

#include <Server.h>

#include "backend/BackendManager.hpp"
#include "memory/SlabMemManager.hpp"

#define META_CACHE_MAX   512 * 1024
#define BLOCK_CACHE_MAX  512 * 1024
/**
 * 用于延时刷新数据到后台的时间，如果异步写入数据后，没有调用 flush，启动定时器延迟自动启动 flush
 */
#define TIMER_FLUSH_TTL 2 * 60

/**
 * 检查数据块缓存超时时间
 */
#define TIMER_TRUSH_TTL  60

class SlabChainOp;
class SlabFileManager;

/**
 * 用于计算网络
 */
class NetworkSpeed {
private:
	time_t start_tm_sec { 0 };
	time_t start_tm_usec { 0 };
	time_t used_tm_usec[5] { 0 }; //使用 Microseconds

	int index { 0 };
	int count { 0 };
public:
	NetworkSpeed() {
		Start();
	}

	void Start() {
		struct timeval tv;
		gettimeofday(&tv, NULL);
		start_tm_sec = tv.tv_sec;
		start_tm_usec = tv.tv_usec;
	}

	time_t Done() {
		struct timeval tv;
		gettimeofday(&tv, NULL);

		time_t t = (tv.tv_sec * 1000000 + tv.tv_usec) - (start_tm_sec * 1000000 + start_tm_usec);
		used_tm_usec[index++] = t;
		if (index >= 5) {
			index = 0;
		}
		if (count < 5) {
			count++;
		}
		return t;
	}

	time_t GetUsedTmUsec() const {
		if (count == 0) {
			return 9999;
		}
		time_t t = 0;
		for (int i = 0; i < count; i++) {
			t += used_tm_usec[i];
		}
		return t / count;
	}
};

/**
 * 每个缓存块对象的元数据信息
 * 在本地缓存 METACACHETTL s
 */
class SlabMeta {
public:
	int32_t version; //远程版本
	bool bReport { true }; //对于读数据，是否需要告诉元数据节点，这儿有数据？

	std::list<std::shared_ptr<SlabPeer>> oSlabPeers; //位置节点

	void CopyFrom(const SlabMeta * pSlabMeta) {
		version = pSlabMeta->version;
		bReport = pSlabMeta->bReport;

		for (auto iter = pSlabMeta->oSlabPeers.begin(); iter != pSlabMeta->oSlabPeers.end(); iter++) {
			oSlabPeers.push_back(*iter);
		}
	}
};

/**
 * 每个缓存块对象所在的文件对象
 * 相同文件在多线程调用，需要考虑线程安全问题
 * 不同文件之间不需考虑
 */
class SlabFile: public std::enable_shared_from_this<SlabFile> {
private:
	/**
	 * 一个文件包含多个 SlabBlock，key 为块索引
	 */

	cache::lru_cache_count_num<size_t, std::weak_ptr<SlabBlock>> oSlabBlocks; //会被多线程修改

	mtsafe::thread_safe_map<size_t, std::weak_ptr<SlabBlock>> oDirtyBlocks; //被修改块，会被多线程修改

	mtsafe::thread_safe_map<size_t, std::shared_ptr<mtsafe::CallBarrier<bool>>> oCallBarriers; //会被多线程修改

	std::shared_ptr<mtsafe::CallBarrier<bool>> oCallBarrier;
	/**
	 * 缓存每个数据块的元数据信息
	 */
	cache::lru_cache_count_num<size_t, std::shared_ptr<SlabMeta> > oSlabMetas; //会被多线程修改

	SlabFileManager * pManager { NULL };
	std::shared_ptr<SlabFactory> oSlabFactory;

	std::shared_ptr<boost::asio::io_service> io_service;
	std::shared_ptr<boost::asio::io_context::strand> io_strand;

	std::string filename; //文件名称，只读
	bool bMemoryFile { false }; //是否为内存文件，只读

	std::atomic<int32_t> uuid; //会被多线程修改

	std::atomic<time_t> nLastActivity { 0 };
	std::atomic<time_t> nLastStatUpdated { 0 }; //属性最后更新时间

	std::atomic<time_t> stat_mtime { 0 };  // 会被多线程修改
	std::atomic<off_t> stat_size { 0 }; // 会被多线程修改

	/**
	 * 检查文件是否超时
	 */
	std::shared_ptr<boost::asio::deadline_timer> timer_Flush;
	std::mutex mtx_flush;
protected:

	void FlushDirtyOne(const std::function<void(ssize_t state, const std::string & message)> & callback = nullptr);

	void ClearDirty();

public:

	bool StatEmpty() {
		/**
		 * 判断是否为空文件
		 */
		time_t mtime = stat_mtime;
		return mtime == 0;
	}

	bool StatValid(uint32_t ttl) {
		time_t mtime = stat_mtime;
		if (mtime == 0) {
			return false;
		}

		size_t used_time = SystemUtils::now() - nLastStatUpdated;
		return used_time < ttl;
	}

	void StatUpdate() {
		nLastStatUpdated = SystemUtils::now();
	}

	void Update() {
		nLastActivity = SystemUtils::now();
	}

	bool IsTimeout(int ttl) {
		int used_time = SystemUtils::now() - nLastActivity;
		return used_time > ttl;
	}

	const time_t GetLastActivity() const {
		return nLastActivity;
	}

public:
	SlabFile(SlabFileManager * pManager_, const std::shared_ptr<SlabFactory>& oSlabFactory_,
			const std::string & filename_, bool memory_);

	/**
	 * 存在一个问题就是当 client 异步写入数据后，没有调用 flush，由于大批量读写新文件，导致在 定时器启动 flush 之前
	 * 由于slabfile数量太多，超过 lrucache 容量，导致 当前 slabfile 被自动 清除了，写入的数据可能未被持久化
	 */
	~SlabFile();

	/**
	 * 清除被回收后的无效缓存块对象引用
	 */
	size_t TraceTrushes();

	/**
	 * 启动延迟 flush
	 */
	void ResetLazyFlush();

	/**
	 * 取消延迟 flush
	 */
	void CancelLazyFlush();

	/**
	 * 针对相同文件相同块提供串行调用控制
	 */

	std::shared_ptr<mtsafe::CallBarrier<bool>> GetBarrier(size_t block_offset_id);
	/**
	 * 针对相同文件提供串行调用控制
	 * GetAttr, Flush
	 */
	std::shared_ptr<mtsafe::CallBarrier<bool>> GetBarrier();

	///////////////////////////
	void PutMeta(size_t block_offset_id, const SlabMeta & oSlabMeta, uint32_t ttl);

	bool GetMeta(size_t block_offset_id, std::shared_ptr<SlabMeta> & oSlabMeta);

	void RemoveMeta(size_t block_offset_id);

	void ClearMeta();

	void ClearAttr();

	///////////////////////////
	void AddBlock(size_t block_offset_id, const std::shared_ptr<SlabBlock>& oSlabBlock);

	std::shared_ptr<SlabBlock> GetBlock(size_t block_offset_id);

	/**
	 * 异步写入时候，标记块为 弄需要写入后端
	 */
	void AddDirty(size_t block_offset_id, const std::shared_ptr<SlabBlock>& oSlabBlock);

	/**
	 * 手动刷新或定时刷新
	 * 刷新更新数据到后端
	 *
	 * 存在多线程进入问题，通过标记来去重
	 * 异步调用
	 */
	void FlushDirty(const std::function<void(ssize_t state, const std::string & message)> & callback = nullptr);

	/**
	 * 由于本地版本低于远程，需要 释放内存资源，并回收，重新获取数据后然后写入
	 *
	 */
	void RemoveBlock(size_t block_offset_id);

	void ClearBlocks();

	///////////////////////////

	const std::string& GetFilename() const {
		return filename;
	}

	const int32_t GetUuid() {
		return uuid.load();
	}

	void SetUuid(int32_t uuid) {
		this->uuid.store(uuid);
	}

	bool IsMemoryFile() const {
		return bMemoryFile;
	}

	const time_t GetStatMtime() const {
		return stat_mtime.load();
	}

	void SetStatMtime(const time_t mtime) {
		stat_mtime.store(mtime);
	}

	const off_t GetStatSize() const {
		return stat_size.load();
	}

	void SetStatSize(const off_t size) {
		stat_size.store(size);
	}
};

#include "SlabChainOp.h"

class SlabFileManager: public std::enable_shared_from_this<SlabFileManager> {
public:
	friend class SlabChainOp;
	friend class SlabChainReader;
	friend class SlabChainWriter;
	friend class SlabFile;
private:
	std::shared_ptr<SlabServerData> oServerData;
	cache::lru_cache_count_num<std::string, std::shared_ptr<SlabFile> > oSlabFiles_m;

	std::shared_ptr<SlabFactory> oSlabFactory;
	std::shared_ptr<BackendManager> oBackendManager;

	std::shared_ptr<TcpMessager> oSlabMessager;

	/**
	 * 定时汇报节点状态
	 */
	std::shared_ptr<boost::asio::deadline_timer> timer_Status;

	/**
	 * 检查文件垃圾对象
	 */
	std::shared_ptr<boost::asio::deadline_timer> timer_Trush;

	std::shared_ptr<boost::asio::io_service> io_service;
	std::shared_ptr<boost::asio::io_context::strand> io_strand;

	std::string meta_addr;
	unsigned short int slab_port;
	unsigned short int meta_port;

	NetworkSpeed oNetworkSpeed;
public:
	typedef std::function<void(time_t stat_mtime, off_t stat_size, int e_code, const std::string & e_message)> GetAttrCallback;

protected:

	void ReportStatus();

	void TraceTrushes(float factor);

	std::shared_ptr<TcpMessage> NewMetaMessage(int action);

	int CalcOffsetBlocks(off_t offset, size_t size, std::vector<uint32_t>& BlockOffsetIds);

	void ResponseEcho(int8_t action, ssize_t bytes_state, const std::string & message_or_data,
			const std::shared_ptr<asio_server_tcp_connection>& conn);

	void PostMessage(const std::shared_ptr<TcpMessage> & message);

	/**
	 * 修改文件的元数据：version, mtime & size
	 */
	void UpdateSlabMeta(uint32_t block_offset_id, const std::shared_ptr<SlabFile> & oSlabFile,
			const std::shared_ptr<SlabBlock> & oSlabBlock);

	/**
	 * 修改文件的属性：mtime & size
	 */
	void UpdateSlabAttr(const std::shared_ptr<SlabFile> & oSlabFile, time_t stat_mtime, off_t newsize, bool trunc_it);

	/**
	 * 获取文件的属性：mtime & size，由于文件可能被其他机器修改，导致长度等变化，优先从本地缓存获取，如无
	 * 针对相同文件采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
	 * 对于 持久文件 直接从 后端存储读取，对于 内存文件 从服务器上获取
	 */
	void EnsureGetAttr(const std::shared_ptr<SlabFile>& oSlabFile, const GetAttrCallback & callback);

	/**
	 * 与服务器文件不一致，需要重置
	 */
	void CheckFileUuid(int32_t iMetaUuid, const std::shared_ptr<SlabFile>& oSlabFile);
public:

	SlabFileManager(const std::shared_ptr<SlabFactory> & oSlabFactory_,
			const std::shared_ptr<SlabServerData> & oServerData_,
			const std::shared_ptr<BackendManager>& oBackendManager_, const std::string & meta_addr_,
			unsigned short int slab_port_, unsigned short int meta_port_);

	~SlabFileManager();

	/*
	 * 写入数据过程:
	 * 先检查本地内存是否有数据，如果本地内存有数据，检查本地内存数据版本和元数据节点的数据版本是否一致，如果一致，直接写入新数据，
	 * 通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
	 * 如果本地内存无数据，需要先读取数据到本地内存，然后写入新数据，通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
	 *
	 * 写入数据，会先清除状态缓存
	 *
	 * 往相同文件同时写入无法保证数据准确性
	 */
	bool Write(const std::string & filename, off_t offset, const std::string & data, bool write_async,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * 读取数据过程:
	 * 先在本地内存找数据块，如果本地内存有数据，检查本地内存数据版本与元数据版本是否一致，如果不一致删除本地内存数据，如果一致直接使用;
	 * 如果本地内存没有，在元数据节点寻找数据的位置分布情况和最新版本号，根据位置分布情况，依次去相应节点的内存找数据，
	 * 如果是最新数据，获取该节点数据副本到本地内存，同时更新元数据副本信息; 如果其它节点不是最新数据，删除该节点数据，更新元数据相应信息;
	 * 如果其它节点都无数据，从后台存储读取数据，放入本地内存，更新元数据数据信息。
	 */
	bool Read(const std::string & filename, int8_t readonly, off_t offset, size_t size,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * Read2 函数实现，读取单独一块数据，在client进行组装
	 */
	bool Read2(const std::string & filename, int8_t readonly, int32_t offset_id,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * 获取文件的属性：mtime & size，由于文件可能被其他机器修改，导致长度等变化，优先从本地缓存获取，如无
	 * 针对相同文件采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
	 * 对于 持久文件 直接从 后端存储读取，对于 内存文件 从服务器上获取
	 */
	bool GetAttr(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 关闭文件
	 */
	bool Close(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 截取文件
	 */
	bool Truncate(const std::string & filename, off_t newsize,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 删除数据以及存储数据
	 */
	bool Unlink(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 删除目录
	 */
	bool RmDir(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 建立目录
	 */
	bool MkDir(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 *
	 * 刷新缓存
	 */
	bool Flush(const std::string & filename, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * 邻居读取整个块
	 */
	ResultType PeerReadSlab(const std::string& filename, int32_t iMetaUuid, uint32_t block_offset_id, int32_t mVersion,
			std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * 元数据服务主动来检查文件的块是否有效，如果无效，本地删除同时反馈元数据进行删除
	 */

	ResultType CheckItSlab(const std::shared_ptr<stringbuffer>& input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection>& conn);

	void start();

	void stop();
};

#endif /* SLABFILEMANAGER_HPP_ */
