/*
 * CacheMetaManager.hpp
 *
 *  Created on: Oct 15, 2018
 *      Author: root
 */

#ifndef CACHEMETAMANAGER_HPP_
#define CACHEMETAMANAGER_HPP_

#include <memory>
#include <atomic>
#include <json/json.h>

#include <databox/mtsafe_object.hpp>
#include <databox/lrucache.hpp>

#include <databox/stringutils.hpp>
#include <databox/filesystemutils.hpp>
#include <databox/stringbuffer.hpp>
#include <databox/async_io_service.hpp>

#include <databox/async_messager.hpp>

#include <header.h>
#include <Server.h>

#include "MetaFile.hpp"
#include "MetaLogger.hpp"

/**
 * 存储节点调线超时时间
 */
#define  NODE_TIMEOUT   3
#define  FILE_TIMEOUT   2 * 60

class SlabPeerData: public SlabPeer {
public:
	SlabPeerData(const std::string & host_, unsigned short int port_) :
			SlabPeer::SlabPeer(host_, port_) {
		nLastActivity = SystemUtils::now();
	}

	/**
	 * 解析节点资源情况
	 */
	bool ParseStatus(const std::shared_ptr<stringbuffer> & input);

	void Update() {
		nLastActivity = SystemUtils::now();
	}

	bool IsTimeout(int ttl) {
		int used_time = SystemUtils::now() - nLastActivity;
		return used_time > ttl;
	}

	uint32_t GetBlockSize() const {
		return block_size;
	}

	time_t GetLastActivity() const {
		return nLastActivity;
	}

	uint64_t GetNetworkDelayUsec() const {
		return network_delay_usec;
	}

	size_t GetSwapFreeBlocks() const {
		return swap_free_blocks;
	}

	size_t GetSwapReadBlocks() const {
		return swap_read_blocks;
	}

	size_t GetSwapWriteBlocks() const {
		return swap_write_blocks;
	}

	size_t GetMemFreeBlocks() const {
		return mem_free_blocks;
	}

	size_t GetMemReadBlocks() const {
		return mem_read_blocks;
	}

	size_t GetMemWriteBlocks() const {
		return mem_write_blocks;
	}

	size_t GetNumFiles() const {
		return num_files;
	}
private:
	time_t nLastActivity { 0 };

	uint32_t block_size { 0 }; // 状态 缓存块尺寸

	size_t mem_read_blocks { 0 }; //状态 使用块数
	size_t mem_write_blocks { 0 }; //状态 缓存块数
	size_t mem_free_blocks { 0 }; //状态 空闲块数

	size_t swap_read_blocks { 0 }; //状态 使用块数
	size_t swap_write_blocks { 0 }; //状态 缓存块数
	size_t swap_free_blocks { 0 }; //状态 空闲块数

	size_t num_files { 0 }; /* 总文件数量 */

	std::atomic<uint64_t> network_delay_usec { 0 }; //网络通讯时间
};

class StatusContainer {
private:
	mtsafe::thread_safe_map<std::string, std::string> oStatusData;
public:
	void add(const std::string & key, const std::string & val);

	bool get(const std::string & key, std::string & val);
};

class MetaFileManager: public std::enable_shared_from_this<MetaFileManager> {
private:
	std::shared_ptr<MetaServerData> oServerData;
	cache::lru_cache_count_num<std::string, std::shared_ptr<MetaFile>> oMetaFiles_m;

	mtsafe::thread_safe_map<std::string, std::shared_ptr<SlabPeerData> > oSlabPeers_m;
	mtsafe::thread_safe_queue<std::string> oSlabPeerNames;

	std::shared_ptr<boost::asio::io_service> io_service;

	std::shared_ptr<boost::asio::deadline_timer> timer_Nodes;

	/**
	 * 消息发送链，每次发送一条，如果消息队列内容太多，需要降低速度
	 */
	std::mutex mtx_loop;
	bool bMessagerLoop { false };

	mtsafe::thread_safe_queue<std::shared_ptr<TcpMessage> > oSlabMessages;
	std::shared_ptr<TcpMessager> oSlabMessager;

	std::shared_ptr<MetaLogger> oMetaLogger;

	/**
	 * 检查文件垃圾对象
	 */
	std::shared_ptr<boost::asio::deadline_timer> timer_Trush;

	bool bTerminating { false };
	StatusContainer oStatusContainer;

protected:
	/**
	 * 定时巡检存储节点，删除超时
	 */
	void TraceNodes();

	void TraceTrushes(float factor);

	void EnterMessageLoop();

	void LeaveMessageLoop();

public:

	std::string GetStatus(const std::string & key);

public:

	MetaFileManager(const std::shared_ptr<MetaServerData>& serverdata_,
			const std::shared_ptr<MetaLogger> & meta_logger_);

	~MetaFileManager();

	void PostMessage(const std::shared_ptr<TcpMessage> & message);

	std::shared_ptr<MetaFile> GetOrCreate(const std::string& filename, int32_t uuid);

	std::shared_ptr<MetaFile> Get(const std::string& filename);

	void Remove(const std::string& filename);

	bool GetSlabPeer(const std::string & peer, uint64_t & ns_usec);

	/**
	 * 解析节点资源情况，如果是新节点进行资源注册
	 */
	bool NodeStatusPost(std::string& rs_host, unsigned short int rs_port, const std::shared_ptr<stringbuffer> & input);

	void start();

	void stop();
};

#endif /* CACHEMETAMANAGER_HPP_ */
