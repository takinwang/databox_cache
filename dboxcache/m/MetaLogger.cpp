/*
 * MetaLogger.cpp
 *
 *  Created on: Jan 31, 2019
 *      Author: root
 */

#include <thread>

#include "MetaLogger.hpp"

#include <databox/cpl_debug.h>

#define CRITICAL_ERROR( a ) \
	SYSLOG_ERROR( a ) \
	LOGGER_ERROR( a )

LeveldbContext::LeveldbContext(const std::string& path_) :
		meta_path(path_) {
	leveldb::Options options;
	options.create_if_missing = true;

	leveldb::Status s = leveldb::DB::Open(options, meta_path, &level_metadb);
	if (s.ok() == false || level_metadb == NULL) {
		const std::string message = s.ToString();
		CRITICAL_ERROR("#" << __LINE__ << ", LeveldbContext::LeveldbContext, open: " << meta_path << ", " << message)
		exit(-1);
	}
}

LeveldbContext::~LeveldbContext() {
	if (level_metadb) {
		delete level_metadb;
		level_metadb = NULL;
		LOGGER_TRACE("#" << __LINE__ << ", LeveldbContext::~LeveldbContext, close: " << meta_path);
	} else {
		LOGGER_TRACE("#" << __LINE__ << ", LeveldbContext::~LeveldbContext");
	}
}

int LeveldbContext::del_log(const std::string& key) {
	if (level_metadb == NULL) {
		LOGGER_ERROR("#" << __LINE__ << ", LeveldbContext::del_log, null")
		return -1;
	}
	std::string start_key = key + ":";

	leveldb::WriteOptions wo;
	leveldb::Status s = level_metadb->Delete(wo, start_key);
	if (s.ok() == false) { //元数据没有找到
		return 0;
	}
	return 1;
}

int LeveldbContext::get_log(const std::string& key, std::string& keydata) {
	if (level_metadb == NULL) {
		LOGGER_ERROR("#" << __LINE__ << ", LeveldbContext::del_log, null")
		return -1;
	}

	std::string start_key = key + ":";

	leveldb::ReadOptions ro;
	leveldb::Status s = level_metadb->Get(ro, start_key, &keydata);
	if (s.ok() == false) { //元数据没有找到
		return 0;
	}
	return 1;
}

int LeveldbContext::put_log(const std::string& key, const std::string& keydata) {
	if (level_metadb == NULL) {
		LOGGER_ERROR("#" << __LINE__ << ", LeveldbContext::put_log, null")
		return -1;
	}

	std::string start_key = key + ":";

	leveldb::WriteOptions wo;
	leveldb::Status s = level_metadb->Put(wo, start_key, keydata);
	if (s.ok() == false) {
		const std::string message = s.ToString();
		CRITICAL_ERROR("#" << __LINE__ << ", LeveldbContext::put_log, error, put: " << key << ", " << message)
		exit(-1);
		return 0;
	} else {
		LOGGER_TRACE("#" << __LINE__ << ", LeveldbContext::put_log, success, put: " << key);
		return 1;
	}
}

int LeveldbContext::iter_logs(
		std::function<bool(int index, const std::string & key, const std::string & val)> const & callback) {
	if (level_metadb == NULL) {
		LOGGER_ERROR("#" << __LINE__ << ", LeveldbContext::iter_logs, null")
		return -1;
	}

	if (callback == nullptr) {
		return 0;
	}

	leveldb::ReadOptions ro;
	ro.snapshot = level_metadb->GetSnapshot();

	leveldb::Iterator * iter = level_metadb->NewIterator(ro);

	iter->SeekToFirst();

	leveldb::Status s;
	int nCount = 0;
	while (iter->Valid()) {
		const std::string& key = iter->key().ToString();
		const std::string& val = iter->value().ToString();

		if (callback(nCount, key, val) == false) {
			break;
		}

		nCount++;
		iter->Next();
	}

	delete iter;

	level_metadb->ReleaseSnapshot(ro.snapshot);
	return nCount;
}

int LeveldbContext::del_logs(const std::string& key) {
	if (level_metadb == NULL) {
		LOGGER_ERROR("#" << __LINE__ << ", LeveldbContext::del_logs, null")
		return -1;
	}

	std::string start_key = key + ":";
	//	const leveldb::Comparator * cmp = leveldb::BytewiseComparator();

	leveldb::ReadOptions ro;
	ro.snapshot = level_metadb->GetSnapshot();

	leveldb::Iterator * iter = level_metadb->NewIterator(ro);

	leveldb::WriteBatch wb;
	leveldb::WriteOptions wo;
	leveldb::Status s;

	leveldb::Slice start_slice(start_key);

	std::string end_key = key + ";";
	leveldb::Slice end_slice(end_key);

	iter->Seek(start_slice);

	int icount = 0;
	int ncount = 0;

	while (iter->Valid()) {
		const leveldb::Slice& l_key = iter->key();
		int cmp_v = l_key.compare(end_slice); // cmp->Compare(key, end_slice);

		if (cmp_v >= 0) {
			break;
		}

		wb.Delete(l_key);

		if (icount++ >= 1024) {
			icount = 0;

			LOGGER_TRACE("#" << __LINE__ << ", LeveldbContext::del_logs: " << key << ", " << ncount)
			s = level_metadb->Write(wo, &wb);
			wb.Clear();

			if (s.ok() == false) {
				break;
			}
		}

		ncount++;
		iter->Next();
	}

	if (icount > 0 && s.ok() == true) {
		s = level_metadb->Write(wo, &wb);
		wb.Clear();
	}

	delete iter;
	level_metadb->ReleaseSnapshot(ro.snapshot);

	if (s.ok() == true) {
		LOGGER_INFO("#" << __LINE__ << ", LeveldbContext::del_logs: " << key << ", " << ncount);
		return 1;
	} else {
		const std::string message = s.ToString();
		CRITICAL_ERROR("#" << __LINE__ << ", LeveldbContext::del_logs, error: " << message)
		exit(-1);
		return 0;
	}
}

MetaLogger::MetaLogger(const std::string& path) :
		sPath(path) {

	LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::MetaLogger, " << sPath)

	if (FileSystemUtils::MakeDirs(sPath) == false) {
		LOGGER_ERROR("#" << __LINE__ << ", MetaLogger::MetaLogger, Not Exist: " << sPath)
		SYSLOG_ERROR("#" << __LINE__ << ", MetaLogger::MetaLogger, Not Exist: " << sPath)
		return;
	}

	oLeveldbContext = std::make_shared<LeveldbContext>(sPath);

	if (oLeveldbContext->isOk() == true) {
		bOk = true;

		oTimer = std::make_shared<ThreadTimer>(std::bind(&MetaLogger::async_write, this, std::placeholders::_1),
				"AsyncWriter");

		oTimer->start(2);
	}
}

MetaLogger::~MetaLogger() {
	LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::~MetaLogger, start ...")

	if (oTimer.get()) {
		oTimer->stop();
	}

	oLeveldbContext.reset();

	LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::~MetaLogger, done: " << sPath)
}

bool MetaLogger::async_write(bool timeout) {
	if (timeout == true || bOk == false) {
		return true;
	}

	std::shared_ptr<MetaLogData> logdata;
	if (oLogDatas.pop_front(logdata) == false) {
//		LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::async_write, Nothing")
		//wait
		return true;
	}

	sync_write(logdata);

	return false;
}

int MetaLogger::sync_write(const std::shared_ptr<MetaLogData> & logdata) {
	stringbuffer s_val;
	std::stringstream s_key;

	if (logdata->action == MetaAction::maRemoveBlock) {
		s_key << logdata->filename << ":" << logdata->off_id << ":" << logdata->peer;

		return oLeveldbContext->del_log(s_key.str());
	}

	if (logdata->action == MetaAction::maPutBlock) {
		s_key << logdata->filename << ":" << logdata->off_id << ":" << logdata->peer << ":01";

		s_val.write_int32(logdata->uuid);
		s_val.write_int32(logdata->version);
		s_val.write_uint16(logdata->port);

		return oLeveldbContext->put_log(s_key.str(), s_val.str());
	}

	if (logdata->action == MetaAction::maRemoveFile) {
		s_key << logdata->filename;
		return oLeveldbContext->del_logs(s_key.str());

	}

	if (logdata->action == MetaAction::maPutFile) {
		s_key << logdata->filename << ":00";
		s_val.write_int32(logdata->uuid);
		s_val.write_int64(logdata->mtime);
		s_val.write_int64(logdata->newsize);

		return oLeveldbContext->put_log(s_key.str(), s_val.str());
	}

	return 0;
}

void MetaLogger::Load(std::function<bool(const std::shared_ptr<MetaLogData> & logdata)> const & callback) {
	if (callback == nullptr) {
		return;
	}

	if (bOk == false) {
		return;
	}

	const auto & IterCallback =
			[ callback ](int index, const std::string & key, const std::string & val ) {

				std::string s_key = key;

				size_t len = s_key.size();
				if (len < 4) {
					return true;
				}

				/* :1: */
				const std::string act = s_key.substr( len - 4 );
				s_key.resize( len - 4 );

				if ( act == ":00:" ) {

					std::shared_ptr<MetaLogData> logdata(new MetaLogData);
					stringbuffer s_val;
					/** file **/
					logdata->filename = s_key;
					logdata->action = MetaAction::maPutFile;

					s_val.str( val );
					if( s_val.read_int32(logdata->uuid) == false || s_val.read_int64(logdata->mtime) == false ||
							s_val.read_int64(logdata->newsize) == false ) {
						return true;
					}

					LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::Load File, " << key << " uuid: " << logdata->uuid << ", mtime: " << logdata->mtime << ", newsize: " << logdata->newsize)

					return callback( logdata );
				} else if ( act == ":01:" ) {

					std::shared_ptr<MetaLogData> logdata(new MetaLogData);
					stringbuffer s_val;
					/** block */
					size_t peer_pos = s_key.rfind(":");
					if (peer_pos == s_key.npos) {
						return true;
					}
					logdata->peer = s_key.substr( peer_pos + 1 );
					s_key.resize( peer_pos );

					size_t offid_pos = s_key.rfind(":");
					if (offid_pos == s_key.npos) {
						return true;
					}
					std::string offid_str = s_key.substr( offid_pos + 1 );
					s_key.resize( offid_pos );

					long block_id = StringUtils::ToLong( offid_str, -1 );
					if(block_id < 0) {
						return true;
					}

					logdata->off_id = block_id;
					logdata->filename = s_key;

					logdata->action = MetaAction::maPutBlock;

					s_val.str( val );
					if( s_val.read_int32(logdata->uuid) == false || s_val.read_int32(logdata->version) == false ||
							s_val.read_uint16(logdata->port) == false ) {
						return true;
					}

					LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::Load Block, " << key << " uuid: " << logdata->uuid << ", v: " << logdata->version << ", port: " << logdata->port)

					return callback( logdata );
				}
				return true;
			};

	LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::Load CachedLogs ...")

	oLeveldbContext->iter_logs(IterCallback);

	LOGGER_TRACE("#" << __LINE__ << ", MetaLogger::Load CachedLogs done.")
}

void MetaLogger::PutFile(const std::string& filename, int32_t uuid, time_t mtime, off_t newsize) {

	if (bOk == false) {
		return;
	}

	std::shared_ptr<MetaLogData> logdata(new MetaLogData);
	logdata->action = MetaAction::maPutFile;
	logdata->filename = filename;
	logdata->uuid = uuid;
	logdata->mtime = mtime;
	logdata->newsize = newsize;

	Store(logdata);
}

void MetaLogger::RemoveFile(const std::string& filename) {

	if (bOk == false) {
		return;
	}

	std::shared_ptr<MetaLogData> logdata(new MetaLogData);
	logdata->action = MetaAction::maRemoveFile;
	logdata->filename = filename;

	Store(logdata);
}

void MetaLogger::PutBlock(const std::string& filename, int32_t uuid, uint32_t block_offset_id, int32_t lVersion,
		const std::string& peer, uint16_t port) {

	if (bOk == false) {
		return;
	}

	std::shared_ptr<MetaLogData> logdata(new MetaLogData);
	logdata->action = MetaAction::maPutBlock;
	logdata->filename = filename;
	logdata->uuid = uuid;
	logdata->off_id = block_offset_id;
	logdata->version = lVersion;
	logdata->peer = peer;
	logdata->port = port;

	Store(logdata);
}

void MetaLogger::RemoveBlock(const std::string& filename, int32_t uuid, uint32_t block_offset_id,
		const std::string& peer) {

	if (bOk == false) {
		return;
	}

	std::shared_ptr<MetaLogData> logdata(new MetaLogData);
	logdata->action = MetaAction::maRemoveBlock;
	logdata->filename = filename;
	logdata->uuid = uuid;
	logdata->off_id = block_offset_id;
	logdata->peer = peer;

	Store(logdata);
}

void MetaLogger::Store(const std::shared_ptr<MetaLogData> & logdata) {

	size_t qsize = oLogDatas.push_back(logdata);
	oTimer->notify_one();

	int nms = qsize / MAX_QSIZE; //待刷新的数据过多，需要降低 put 速度，以方便 flush 线程持久化写入
	if (nms > 0) {

		LOGGER_ERROR("#" << __LINE__ << ", MetaLogger::Store, slow down: " << nms << " ms, queue overflow: " << qsize)

		SYSLOG_ERROR("#" << __LINE__ << ", MetaLogger::Store, slow down: " << nms << " ms, queue overflow: " << qsize)

		std::this_thread::sleep_for(std::chrono::milliseconds(nms));
	}
}
