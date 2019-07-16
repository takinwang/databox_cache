/*
 * CacheFileService.cpp
 *
 *  Created on: Nov 22, 2018
 *      Author: root
 */

#include "CacheFileService.hpp"

CacheFileService::CacheFileService(const std::shared_ptr<MetaServerData>& oServerData_) :
		oServerData(oServerData_) {

	oMetaLogger = std::make_shared<MetaLogger>(oServerData_->meta_path);

	oIoService = AsyncIOService::getInstance();
	oMetaFileManager = std::make_shared<MetaFileManager>(oServerData, oMetaLogger);

	oTcpServer = std::make_shared<AsioTcpServer>(oIoService->getIoService(), oServerData->meta_port,
			std::bind(&CacheFileService::hb_request, this, std::placeholders::_1, std::placeholders::_2,
					std::placeholders::_3, std::placeholders::_4));

	oCacheStatusService = std::make_shared<DefaultStatusService>(oMetaFileManager, oServerData_->status_port);
}

CacheFileService::~CacheFileService() {

}

ResultType CacheFileService::hb_request(std::shared_ptr<stringbuffer> input, std::shared_ptr<stringbuffer> output,
		const boost::system::error_code& ec, std::shared_ptr<base_connection> conn) {

	// 在 hb_request 作用范围内，conn 不会为 NULL
	// return false 关闭连接

	if (ec) {
		return ResultType::rtFailed;
	}

	int8_t action;
	if (input->read_int8(action) == false) {
		return ResultType::rtFailed;
	}

	const std::shared_ptr<asio_server_tcp_connection> & worker_conn = std::static_pointer_cast<
			asio_server_tcp_connection, base_connection>(conn);

	if (action == CacheAction::caSlabStatus) { // 数据节点注册状态信息
		return DoSlabStatusPost(input, output, worker_conn);
	}

	if (action == CacheAction::caSlabGetMeta) { // 数据节点请求块信息
		return DoSlabGetMeta(input, output, worker_conn);
	}

	if (action == CacheAction::caSlabPutMeta) { // 数据节点保存块信息
		return DoSlabPutMeta(input, output, worker_conn);
	}

	if (action == CacheAction::caSlabPutAttr) { // 修改文件属性，只针对内存文件
		return DoSlabPutAttr(input, output, worker_conn);
	}

	if (action == CacheAction::caSlabGetAttr) { // 修改文件属性，只针对内存文件
		return DoSlabGetAttr(input, output, worker_conn);
	}

	if (action == CacheAction::caClientUnlink) { //客户端删除文件
		return DoSlabUnlinkFile(input, output, worker_conn);
	}

	if (action == CacheAction::caClientTruncate) { //客户端截取文件
		return DoSlabTruncateFile(input, output, worker_conn);
	}

	return ResultType::rtFailed;
}

void CacheFileService::run_server() {

	oMetaLogger->Load(std::bind(&CacheFileService::hb_replay_logs, this, std::placeholders::_1));

	LOGGER_TRACE(
			"#" << __LINE__ << ", CacheFileService::run_server, Port: " << oServerData->meta_port << ", Worker: " << oServerData->workers);

	auto self = this->shared_from_this();

	//接收到 ctrl+c 等信号后，io_service 是否还正常工作?
	oIoService->Signal(
			[ this, self ]( boost::system::error_code code, int signum ) {
				LOGGER_INFO("#" << __LINE__ << ", CacheFileService::run_server, Catch Signal: " << signum << ", Shutdown now." );
				this->stop();

			});

	SYSLOG_WARN(
			"#" << __LINE__ << ", CacheFileService::run_server, Port: " << oServerData->meta_port << ", Worker: " << oServerData->workers);

	oTcpServer->start();
	oMetaFileManager->start();
	oCacheStatusService->start();

	oIoService->start(oServerData->workers, false);
	oIoService->join();

	LOGGER_TRACE("#" << __LINE__ << ", CacheManager::run_server, Terminated");
	SYSLOG_WARN("#" << __LINE__ << ", CacheManager::run_server, Terminated");
}

void CacheFileService::stop() {
	if (bTerminating) {
		return;
	}

	bTerminating = true;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::stopping ...");

	oMetaFileManager->stop();
	oCacheStatusService->stop();

	oTcpServer->stop();

	oIoService->stop();
	LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::stopped");

}

bool CacheFileService::hb_replay_logs(const std::shared_ptr<MetaLogData> & logdata) {

	if (logdata->action == MetaAction::maPutFile) {

		const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->GetOrCreate(logdata->filename, logdata->uuid);

		oMetaFile->SetStatMtime(logdata->mtime);
		oMetaFile->SetStatSize(logdata->newsize);

		oMetaFile->Update();

	} else if (logdata->action == MetaAction::maPutBlock) {

		const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->GetOrCreate(logdata->filename, logdata->uuid);

		oMetaFile->Update();

		int32_t iMetaUuid = oMetaFile->GetUuid();

		/**
		 * 如果双方文件版本不一致，不能修改
		 * 发生在元数据文件被删除了，后被写入新同名文件，返回错误就可以了，双方不需要处理，正常不会出现这情况
		 */
		if (logdata->uuid != iMetaUuid) {
			return true;
		}

		const std::shared_ptr<SlabPeer> & oSlabPeer = std::make_shared<SlabPeer>(logdata->peer, logdata->port);

		const std::shared_ptr<MetaBlock> & oMetaBlock = oMetaFile->GetOrCreate(logdata->off_id);

		int32_t mVersion = oMetaBlock->GetVersion();

		if (logdata->version > mVersion) {
			oMetaBlock->SetVersion(logdata->version);
			oMetaBlock->ClearPeers();
		}

		oMetaBlock->AddPeer(oSlabPeer);
	}

	return true;
}

ResultType CacheFileService::DoSlabStatusPost(const std::shared_ptr<stringbuffer> & input,
		const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	unsigned short int rs_port;
	if (input->read_uint16(rs_port) == false) {
		return ResultType::rtFailed;
	}

	std::string rs_host;
	unsigned short int peer_port;
	if (conn->remote_endpoint(rs_host, peer_port) == false) {
		return ResultType::rtFailed;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::DoSlabStatusPost, " << rs_host << ":" << rs_port)
	if (oMetaFileManager->NodeStatusPost(rs_host, rs_port, input) == false) {
		return ResultType::rtFailed;
	}

	output->write_int8(CacheAction::caSlabStatusResp);

	return ResultType::rtSuccess;
}

/**
 * 处理 数据节点 请求 数据块的 的版本信息，如果不存在，返回 0，否则 version
 * 如果 slab 文件的 uuid 与 元数据文件 的 uuid 不一致，则 slab 文件需要抛弃
 * 两种场景下发现：
 * 1、文件被其他人删除了
 * 2、文件被其他人删除了，后又被新建了
 */
ResultType CacheFileService::DoSlabGetMeta(const std::shared_ptr<stringbuffer> & input,
		const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	int32_t uuid;
	std::string filename;
	uint32_t nBlocks;
	uint16_t rs_port;

	if (input->read_int32(uuid) == false || input->read_str(filename) == false || input->read_uint32(nBlocks) == false
			|| input->read_uint16(rs_port) == false) {
		return ResultType::rtFailed;
	}

	std::string rs_host;
	uint16_t peer_port;

	if (conn->remote_endpoint(rs_host, peer_port) == false) {
		return ResultType::rtFailed;
	}

	output->write_int8(CacheAction::caSlabGetMetaResp);

	const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->Get(filename);

	if (oMetaFile.get() == NULL) { //文件没有
		LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::DoSlabGetMeta, Not found: " << filename)
		output->write_int32(0); //uuid
		output->write_uint32(0); //nBlocks
		return ResultType::rtSuccess;
	}

	oMetaFile->Update();

	output->write_int32(oMetaFile->GetUuid());
	output->write_uint32(nBlocks);

	for (uint32_t idx = 0; idx < nBlocks; idx++) {
		uint32_t block_offset_id;
		if (input->read_uint32(block_offset_id) == false) {
			return ResultType::rtFailed;
		}

		LOGGER_TRACE(
				"#" << __LINE__ << ", CacheFileService::DoSlabGetMeta: " << filename << ", " << nBlocks << " blocks, No." << idx << ", " << block_offset_id)

		output->write_uint32(block_offset_id);

		const std::shared_ptr<MetaBlock> & oMetaBlock = oMetaFile->Get(block_offset_id);

		if (oMetaBlock.get() == NULL) {  //数据块没有
			output->write_int32(0);  //version
			output->write_uint32(0); //peers
			continue;
		}

		output->write_int32(oMetaBlock->GetVersion());

		std::vector<std::string> peers;
		oMetaBlock->GetPeers(peers, rs_host, rs_port);

		std::string best_peer;
		uint64_t delay_usec = 100000000;
		std::vector<std::string> tmp_peers;

		for (const std::string & peer : peers) {
			uint64_t t = 0;
			if (oMetaFileManager->GetSlabPeer(peer, t) == true) {
				if (delay_usec > t) {
					delay_usec = t;
					best_peer = peer;
				}
				tmp_peers.push_back(peer);
			}
		}

		if (filename.find( MEM_PREFIX) == 0) { //内存文件
			output->write_uint32(tmp_peers.size());
			for (const std::string & peer : tmp_peers) {
				output->write_str(peer);
				LOGGER_TRACE(
						"#" << __LINE__ << ", CacheFileService::DoSlabGetMeta: " << filename << ", BlockId: " << block_offset_id << ", Peers: " << tmp_peers.size() << ", Peer: " << peer)
			}
		} else {
			if (delay_usec < 200000) { //ping 响应速度小于 0.2 s，否则直接从 backend 获取数据
				output->write_uint32(1);
				output->write_str(best_peer);
				LOGGER_TRACE(
						"#" << __LINE__ << ", CacheFileService::DoSlabGetMeta: " << filename << ", BlockId: " << block_offset_id << ", Best peer: " << best_peer)
			} else {
				output->write_uint32(0);
			}
		}

	}

	return ResultType::rtSuccess;
}

/**
 * 修改数据块缓存信息，删除数据的其他节点信息，更新版本和数据位置
 */
ResultType CacheFileService::DoSlabPutMeta(const std::shared_ptr<stringbuffer> & input,
		const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	std::string filename;
	uint32_t block_offset_id;
	int32_t lVersion;
	int32_t uuid;

	uint16_t rs_port;

	int8_t overwrite;

	if (input->read_int32(uuid) == false || input->read_str(filename) == false
			|| input->read_uint32(block_offset_id) == false || input->read_int32(lVersion) == false
			|| input->read_uint16(rs_port) == false || input->read_int8(overwrite) == false) {
		return ResultType::rtFailed;
	}

	time_t mtime = 0;
	off_t newsize = 0;

	if (input->read_int64(mtime) == false || input->read_int64(newsize) == false) {
//pass
	}

	std::string rs_host;
	uint16_t peer_port;

	if (conn->remote_endpoint(rs_host, peer_port) == false) {
		return ResultType::rtFailed;
	}

	const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->GetOrCreate(filename, uuid);

	oMetaFile->Update();
	output->write_int8(CacheAction::caSlabPutMetaResp);

	int32_t iMetaUuid = oMetaFile->GetUuid();
	output->write_int32(iMetaUuid);

	/**
	 * 如果双方文件版本不一致，不能修改
	 * 发生在元数据文件被删除了，后被写入新同名文件，返回错误就可以了，双方不需要处理，正常不会出现这情况
	 */
	if (uuid != iMetaUuid) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", CacheFileService::DoSlabPutMeta, UUID not same: " << filename << ", " << uuid << " --- " << iMetaUuid)

		output->write_int32(tsFailed);
		return ResultType::rtSuccess;
	}

	const std::shared_ptr<SlabPeer> & oSlabPeer = std::make_shared<SlabPeer>(rs_host, rs_port);

	const std::shared_ptr<MetaBlock> & oMetaBlock = oMetaFile->GetOrCreate(block_offset_id);

	int32_t mVersion = oMetaBlock->GetVersion();

	LOGGER_TRACE(
			"#" << __LINE__ << ", CacheFileService::DoSlabPutMeta: " << filename << ", BlockId: " << block_offset_id << ", Version: " << mVersion << " --> " << lVersion << ", peer: " << oSlabPeer->getKey())

	if (overwrite == 1) {
		oMetaBlock->ClearPeers();
	}

	if (lVersion > mVersion) {
		oMetaBlock->SetVersion(lVersion);
		oMetaBlock->ClearPeers();
	}

	oMetaBlock->AddPeer(oSlabPeer);

	output->write_int32( tsSuccess);

	oMetaLogger->PutFile(filename, uuid, mtime, newsize);
	oMetaLogger->PutBlock(filename, uuid, block_offset_id, lVersion, rs_host, rs_port);

	return ResultType::rtSuccess;
}

ResultType CacheFileService::DoSlabGetAttr(const std::shared_ptr<stringbuffer>& input,
		const std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	int32_t uuid;
	std::string filename;

	if (input->read_int32(uuid) == false || input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	output->write_int8(CacheAction::caSlabGetAttrResp);

	const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->Get(filename);

	if (oMetaFile.get() == NULL) { //文件没有
//		LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::DoSlabGetAttr, Not found: " << filename)
		output->write_int32(0); //uuid
		output->write_int64(-ENOENT); //mtime
		output->write_int64(0); //size
		return ResultType::rtSuccess;
	}

	int32_t iMetaUuid = oMetaFile->GetUuid();
	output->write_int32(iMetaUuid);

	time_t mtime = oMetaFile->GetStatMtime();
	output->write_int64(mtime);

	off_t size = oMetaFile->GetStatSize();
	output->write_int64(size);

	LOGGER_TRACE(
			"#" << __LINE__ << ", CacheFileService::DoSlabGetAttr: " << filename << ", mtime: " << mtime << ", size: " << size << ", atime: " << oMetaFile->GetLastActivity())

	return ResultType::rtSuccess;
}

/**
 * 修改文件的修改时间和大小，只针对内存文件
 */
ResultType CacheFileService::DoSlabPutAttr(const std::shared_ptr<stringbuffer> & input,
		const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	int32_t uuid;
	std::string filename;

	time_t mtime;
	off_t newsize;

	int8_t trunc_it;

	if (input->read_int32(uuid) == false || input->read_str(filename) == false || input->read_int64(mtime) == false
			|| input->read_int64(newsize) == false || input->read_int8(trunc_it) == false) {
		return ResultType::rtFailed;
	}

	const std::shared_ptr<MetaFile> & oMetaFile = oMetaFileManager->GetOrCreate(filename, uuid);

	oMetaFile->Update();
	output->write_int8(CacheAction::caSlabPutAttrResp);

	int32_t iMetaUuid = oMetaFile->GetUuid();
	output->write_int32(iMetaUuid);

	/**
	 * 如果双方文件版本不一致，不能修改
	 * 发生在元数据文件被删除了，后被写入新同名文件，返回错误就可以了，双方不需要处理，正常不会出现这情况
	 */
	if (uuid != iMetaUuid) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", CacheFileService::DoSlabPutAttr, UUID not same: " << filename << ", " << uuid << " --- " << iMetaUuid)
		output->write_int32(tsFailed);
		return ResultType::rtSuccess;
	}

	if (trunc_it != 1) {
		off_t oldsize = oMetaFile->GetStatSize();
		newsize = std::max<off_t>(newsize, oldsize);
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", CacheFileService::DoSlabPutAttr: " << filename << ", mtime: " << mtime << ", newsize: " << newsize)

	oMetaFile->SetStatMtime(mtime);
	oMetaFile->SetStatSize(newsize);

	output->write_int32( tsSuccess);

	oMetaLogger->PutFile(filename, uuid, mtime, newsize);

	return ResultType::rtSuccess;
}

ResultType CacheFileService::DoSlabUnlinkFile(const std::shared_ptr<stringbuffer>& input,
		const std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	std::string filename;
	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::DoSlabUnlinkFile: " << filename);

	oMetaFileManager->Remove(filename);
	output->write_int8(CacheAction::caClientUnlinkResp);

	oMetaLogger->RemoveFile(filename);

	return ResultType::rtSuccess;
}

ResultType CacheFileService::DoSlabTruncateFile(const std::shared_ptr<stringbuffer>& input,
		const std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	std::string filename;
	off_t newsize;

	if (input->read_str(filename) == false || input->read_int64(newsize) == false) {
		return ResultType::rtFailed;
	}

	LOGGER_TRACE("#" << __LINE__ << ", CacheFileService::DoSlabTruncateFile: " << filename << ", newsize: " << newsize);

	oMetaFileManager->Remove(filename);
	output->write_int8(CacheAction::caClientTruncateResp);

	oMetaLogger->RemoveFile(filename);

	return ResultType::rtSuccess;
}
