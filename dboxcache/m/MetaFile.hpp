/*
 * MetaFile.hpp
 *
 *  Created on: Jan 31, 2019
 *      Author: root
 */

#ifndef M_METAFILE_HPP_
#define M_METAFILE_HPP_

#include <memory>

#include <databox/filesystemutils.hpp>
#include <databox/stringbuffer.hpp>
#include <databox/mtsafe_object.hpp>
#include <databox/lrucache.hpp>

#include <header.h>
#include <Server.h>

class MetaFileManager;

class MetaBlock: public std::enable_shared_from_this<MetaBlock> {
public:
	MetaBlock();

	~MetaBlock();

	void AddPeer(const std::shared_ptr<SlabPeer> & oSlabPeer);

	int GetPeers(std::vector<std::string>& peers);

	int GetPeers(std::vector<std::string>& peers, const std::string & host, unsigned short int port);

	size_t RemovePeer(const std::string & peer);

	size_t RemovePeer(const std::shared_ptr<SlabPeer> & oSlabPeer);

	void ClearPeers();

	const int32_t GetVersion() const {
		return version;
	}

	void SetVersion(const size_t version) {
		this->version = version;
	}

	void Update() {
		nLastActivity = SystemUtils::now();
	}

	const time_t GetLastActivity() const {
		return nLastActivity;
	}

private:
	std::atomic<time_t> nLastActivity { 0 };

	std::atomic<int32_t> version { 0 }; //当前数据版本，会在多线程中读取和修改

	mtsafe::thread_safe_map<std::string, std::shared_ptr<SlabPeer> > oSlabPeers;
};

/**
 * 一个内存文件的元数据信息，存储内存块的索引信息
 */
class MetaFile: public std::enable_shared_from_this<MetaFile> {
private:
	std::shared_ptr<MetaFileManager> oMetaFileManager;

	std::string filename;
	int32_t uuid { 0 }; //用来区分元数据和块存储上是否为相同文件对象，假如

	mtsafe::thread_safe_map<uint32_t, std::shared_ptr<MetaBlock> > oMetaBlocks;
	std::atomic<time_t> nLastActivity { 0 };

	std::atomic<time_t> stat_mtime { time(NULL) }; // 会被多线程修改，只针对内存文件
	std::atomic<off_t> stat_size { 0 }; // 会被多线程修改，只针对内存文件

public:
	MetaFile(const std::shared_ptr<MetaFileManager> & oMetaFileManager_, const std::string & filename_, int32_t uuid_);

	~MetaFile();

	std::shared_ptr<MetaBlock> GetOrCreate(uint32_t block_offset_id);

	std::shared_ptr<MetaBlock> Get(uint32_t block_offset_id);

	void GetKeys(std::vector<uint32_t> & keys);

	void RemovePeer(uint32_t block_offset_id, const std::string & peer);

	const std::string& GetFilename() const {
		return filename;
	}

	size_t GetNumMetaBlocks() {
		return oMetaBlocks.size();
	}

	int32_t GetUuid() const {
		return uuid;
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

	const time_t GetStatMtime() const {
		return stat_mtime;
	}

	void SetStatMtime(const time_t statMtime) {
		stat_mtime = statMtime;
	}

	const off_t GetStatSize() const {
		return stat_size;
	}

	void SetStatSize(const off_t statSize) {
		stat_size = statSize;
	}
};

#endif /* M_METAFILE_HPP_ */
