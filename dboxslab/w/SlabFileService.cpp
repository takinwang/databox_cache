/*
 * SlabFileService.cpp
 *
 *  Created on: Nov 22, 2018
 *      Author: root
 */

#include "SlabFileService.hpp"

SlabFileService::SlabFileService(const std::shared_ptr<SlabServerData>& serverdata_, const ConfReader & conf_) :
		oServerData(serverdata_) {

	oIoService = AsyncIOService::getInstance();

	if (serverdata_->max_swap_slabs >= (2 * serverdata_->max_memory_slabs) && serverdata_->swap_path.size() > 0) {
		std::shared_ptr<SlabMemFactory> oSlabMemFactory = std::make_shared<SlabMemFactory>(
				serverdata_->max_memory_slabs);
		oSlabFactory = std::shared_ptr<SlabFactory>(
				new SlabMMapFactory(oSlabMemFactory, serverdata_->swap_path, serverdata_->max_swap_slabs));
	} else {
		oSlabFactory = std::shared_ptr<SlabFactory>(new SlabMemFactory(serverdata_->max_memory_slabs));
	}

	oBackendManager = std::make_shared<BackendManager>(conf_);

	oSlabFileManager = std::make_shared<SlabFileManager>(oSlabFactory, serverdata_, oBackendManager,
			serverdata_->meta_addr, serverdata_->slab_port, serverdata_->meta_port);

	oSlabTcpServer = std::make_shared<AsioTcpServer>(oIoService->getIoService(), oServerData->slab_port,
			std::bind(&SlabFileService::hb_request, this, std::placeholders::_1, std::placeholders::_2,
					std::placeholders::_3, std::placeholders::_4));

	if (serverdata_->slab_sock.size() > 0) {
		oSlabUnixServer = std::make_shared<AsioUnixServer>(oIoService->getIoService(), oServerData->slab_sock,
				std::bind(&SlabFileService::hb_request, this, std::placeholders::_1, std::placeholders::_2,
						std::placeholders::_3, std::placeholders::_4));
	}
}

SlabFileService::~SlabFileService() {
	oSlabUnixServer.reset();
	oSlabTcpServer.reset();

	oSlabFactory.reset();
	oBackendManager.reset();

	oSlabFileManager.reset();

	oIoService.reset();
}

ResultType SlabFileService::hb_request(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const boost::system::error_code &ec,
		std::shared_ptr<base_connection> conn) {

	// 在 hb_request 作用范围内，conn 不会为 NULL
	// return false 关闭连接

	if (ec) {
		return ResultType::rtFailed;
	}

	int8_t action;
	if (input->read_int8(action) == false) {
		return ResultType::rtFailed;
	}

	std::shared_ptr<asio_server_tcp_connection> worker_conn = std::static_pointer_cast<asio_server_tcp_connection,
			base_connection>(conn);

	if (action == CacheAction::caClientWrite) { // 写入缓存数据
		return DoClientWrite(input, output, worker_conn);
	}

	if (action == CacheAction::caClientRead) { // 读取缓存数据
		return DoClientRead(input, output, worker_conn);
	}

	if (action == CacheAction::caClientRead2) { // 读取缓存数据
		return DoClientRead2(input, output, worker_conn);
	}

	if (action == CacheAction::caSlabPeerRead) { //读取邻居缓存数据
		return DoSlabPeerRead(input, output, worker_conn);
	}

	if (action == CacheAction::caClientUnlink) { //删除文件
		return DoClientUnlink(input, output, worker_conn);
	}

	if (action == CacheAction::caClientTruncate) { //文件截取
		return DoClientTruncate(input, output, worker_conn);
	}

	if (action == CacheAction::caClientGetAttr) { //文件属性
		return DoClientGetAttr(input, output, worker_conn);
	}

	if (action == CacheAction::caClientMkDir) { //建立目录
		return DoClientMkDir(input, output, worker_conn);
	}

	if (action == CacheAction::caClientRmDir) { //删除目录
		return DoClientRmDir(input, output, worker_conn);
	}

	if (action == CacheAction::caClose) { //文件关闭
		return DoClientClose(input, output, worker_conn);
	}

	if (action == CacheAction::caClientFlush) { //文件截取
		return DoClientFlush(input, output, worker_conn);
	}

//	if (action == CacheAction::caAdmin) {
//		return DoClientAdmin(input, output, worker_conn);
//	}

	if (action == CacheAction::caMasterCheckIt) {
		return DoMasterCheckIt(input, output, worker_conn);
	}

	return ResultType::rtFailed;
}

ResultType SlabFileService::DoClientRead(const std::shared_ptr<stringbuffer> & input,
		std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	std::string filename;

	off_t offset;
	size_t buffer_size;

	int8_t readonly;

	if (input->read_str(filename) == false || input->read_int8(readonly) == false || input->read_int64(offset) == false
			|| input->read_uint64(buffer_size) == false) {
		return ResultType::rtFailed;
	}

	if (offset < 0) {
		output->write_int8(CacheAction::caClientReadResp);
		output->write_int32(tsFailed);
		output->write_str("Invalid offset");
		return ResultType::rtSuccess;
	}

	if (buffer_size > MAX_REQUEST_SIZE) {
		output->write_int8(CacheAction::caClientReadResp);
		output->write_int32(tsFailed);
		output->write_str("Request too large");
		return ResultType::rtSuccess;
	}

	if (oSlabFileManager->Read(filename, readonly, offset, buffer_size, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

/**
 * Read2 函数实现，读取单独一块数据，在client进行组装
 */

ResultType SlabFileService::DoClientRead2(const std::shared_ptr<stringbuffer> & input,
		std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	std::string filename;

	int8_t readonly;
	int32_t offset_id;

	if (input->read_str(filename) == false || input->read_int8(readonly) == false
			|| input->read_int32(offset_id) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->Read2(filename, readonly, offset_id, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientWrite(const std::shared_ptr<stringbuffer> & input,
		std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	std::string filename;
	int8_t readonly;

	off_t offset;
	std::string data;

	if (input->read_str(filename) == false || input->read_int8(readonly) == false || input->read_int64(offset) == false
			|| input->read_str(data) == false) {
		return ResultType::rtFailed;
	}

	bool write_async = false;
	int8_t iasync = 0;

	if (input->read_int8(iasync) == true && iasync == 1) {
		write_async = true;
	}

	if (offset < 0) {
		output->write_int8(CacheAction::caClientWriteResp);
		output->write_int32(tsFailed);
		output->write_str("Invalid offset");
		return ResultType::rtSuccess;
	}

//	write_async = false;
	/**
	 * 异步写入
	 */

	if (oSlabFileManager->Write(filename, offset, data, write_async, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoSlabPeerRead(const std::shared_ptr<stringbuffer> & input,
		std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	std::string filename;
	uint32_t block_offset_id;
	int32_t mVersion;
	int32_t uuid;

	if (input->read_int32(uuid) == false || input->read_str(filename) == false
			|| input->read_uint32(block_offset_id) == false || input->read_int32(mVersion) == false) {
		return ResultType::rtFailed;
	}

	return oSlabFileManager->PeerReadSlab(filename, uuid, block_offset_id, mVersion, output, conn);
}

ResultType SlabFileService::DoClientUnlink(const std::shared_ptr<stringbuffer> & input,
		std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn) {
	std::string filename;

	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->Unlink(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientRmDir(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;

	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->RmDir(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientMkDir(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;

	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->MkDir(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientGetAttr(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;

	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->GetAttr(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientTruncate(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;
	off_t newsize;

	if (input->read_str(filename) == false || input->read_int64(newsize) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->Truncate(filename, newsize, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientClose(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;

	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->Close(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

ResultType SlabFileService::DoClientFlush(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	std::string filename;
	if (input->read_str(filename) == false) {
		return ResultType::rtFailed;
	}

	if (oSlabFileManager->Flush(filename, conn) == false) {
		return ResultType::rtFailed;
	}

	return ResultType::rtNothing;
}

//ResultType SlabFileService::DoClientAdmin(const std::shared_ptr<stringbuffer> & input,
//		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection> & conn) {
//
//	int action;
//	if (input->read_int32(action) == false) {
//		return ResultType::rtFailed;
//	}
//
//	return ResultType::rtNothing;
//}

ResultType SlabFileService::DoMasterCheckIt(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	return oSlabFileManager->CheckItSlab(input, output, conn);

}

void SlabFileService::run_server() {
	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileService::run_server, Port: " << oServerData->slab_port << ", max_slabs: " << oServerData->max_memory_slabs << ", Worker: " << oServerData->workers);

	auto self = this->shared_from_this();

//  接收到 ctrl+c 等信号后，io_service 是否还正常工作?
	oIoService->Signal([ this, self ]( boost::system::error_code code, int signum ) {

		LOGGER_INFO(
				"#" << __LINE__ << ", SlabFileService::run_server, Catch Signal: " << signum << ", Shutdown now.");
		this->stop();
	});

	SYSLOG_WARN(
			"#" << __LINE__ << ", SlabFileService::run_server, Port: " << oServerData->slab_port << ", max_slabs: " << oServerData->max_memory_slabs << ", Worker: " << oServerData->workers);

	oSlabFileManager->start();

	oSlabTcpServer->setMaxRequestSize( MAX_REQUEST_SIZE);
	oSlabTcpServer->start();

	if (oSlabUnixServer.get() != NULL) {
		oSlabUnixServer->setMaxRequestSize( MAX_REQUEST_SIZE);
		oSlabUnixServer->start();
	}

	oIoService->start(oServerData->workers, false);
	oIoService->join();

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileService::run_server, Terminated")

	SYSLOG_WARN("#" << __LINE__ << ", SlabFileService::run_server, Terminated")
}

void SlabFileService::stop() {
	if (bTerminating) {
		return;
	}

	bTerminating = true;

	LOGGER_TRACE("#" << __LINE__ << ", CacheManager::stopping ...");

	oSlabFileManager->stop();
	oSlabTcpServer->stop();

	if (oSlabUnixServer.get() != NULL) {
		oSlabUnixServer->stop();
	}

	oIoService->stop();
	LOGGER_TRACE("#" << __LINE__ << ", CacheManager::stopped");
}
