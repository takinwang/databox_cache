/*
 * MetaLogger.hpp
 *
 *  Created on: Jan 30, 2019
 *      Author: root
 */

#ifndef M_METALOGGER_HPP_
#define M_METALOGGER_HPP_

#include <memory>
#include <functional>
#include <vector>

#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/write_batch.h>
#include <leveldb/comparator.h>

#include <databox/filesystemutils.hpp>

#include <databox/cpl_conf.hpp>

#include <databox/thrdtimer.hpp>
#include <databox/stringbuffer.hpp>
#include <databox/cpl_debug.h>
#include <databox/mtsafe_object.hpp>

#define MAX_LOGSIZE   ( 64 * 1024  ) // * 1024
#define NUM_LOGFILES  ( 12  )
#define MAX_QSIZE     ( 1024 )

class LeveldbContext {
private:
	std::string meta_path;
	leveldb::DB * level_metadb { NULL };
public:
	LeveldbContext(const std::string& path_);

	~LeveldbContext();

	int del_log(const std::string & key);

	int get_log(const std::string & key, std::string & keydata);

	int put_log(const std::string & key, const std::string & keydata);

	int del_logs(const std::string& key);

	int iter_logs(std::function<bool(int index, const std::string & key, const std::string & val)> const & callback);

	bool isOk() const {
		if (level_metadb == NULL) {
			return false;
		} else {
			return true;
		}
	}
};

enum MetaAction {
	maPutFile = 0, //
	maRemoveFile = 1, //
	maPutBlock = 2, //
	maRemoveBlock = 3 //
};

struct MetaLogData {
	MetaAction action;

	std::string filename;
	int32_t uuid;

	union {
		struct {
			uint32_t off_id;
			int32_t version;
		};
		time_t mtime;
	};

	union {
		struct {
			uint16_t port;
			uint16_t _nop1_;
			uint32_t _nop2_;
		};
		off_t newsize;
	};

	std::string peer;
};

class MetaLogger {
private:
	mtsafe::thread_safe_queue<std::shared_ptr<MetaLogData>> oLogDatas;

	std::shared_ptr<ThreadTimer> oTimer;

	std::string sPath;

	std::shared_ptr<LeveldbContext> oLeveldbContext;

	bool bOk { false };
protected:
	bool async_write(bool timeout);

	int sync_write(const std::shared_ptr<MetaLogData> & logdata);

	void Store(const std::shared_ptr<MetaLogData> & data);

public:
	MetaLogger(const std::string & path);

	~MetaLogger();

	void Load(std::function<bool(const std::shared_ptr<MetaLogData> & logdata)> const & callback);

	void PutFile(const std::string& filename, int32_t uuid, time_t mtime, off_t newsize);

	void RemoveFile(const std::string & filename);

	void PutBlock(const std::string & filename, int32_t uuid, uint32_t block_offset_id, int32_t lVersion,
			const std::string & peer, uint16_t port);

	void RemoveBlock(const std::string & filename, int32_t uuid, uint32_t block_offset_id, const std::string & peer);
};

#endif /* M_METALOGGER_HPP_ */
