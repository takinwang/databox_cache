/*
 * SlabFileManager.cpp
 *
 *  Created on: Nov 22, 2018
 *      Author: root
 */

#include <header.h>

#include "SlabFileManager.hpp"

#include "SlabChainOp.inc"
#include "SlabFile.inc"

SlabFileManager::SlabFileManager(const std::shared_ptr<SlabFactory> & oSlabFactory_,
		const std::shared_ptr<SlabServerData> & oServerData_, const std::shared_ptr<BackendManager>& oBackendManager_,
		const std::string & meta_addr_, unsigned short int slab_port_, unsigned short int meta_port_) :
		oServerData(oServerData_), oSlabFiles_m(oServerData_->max_files), oSlabFactory(oSlabFactory_), //
		oBackendManager(oBackendManager_), meta_addr(meta_addr_), slab_port(slab_port_), meta_port(meta_port_) {

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::SlabFileManager, MaxFiles: " << oServerData->max_files);

	const auto & ios = AsyncIOService::getInstance();

	io_service = ios->getIoService();
	io_strand = ios->getStrand();

	timer_Status = std::make_shared<boost::asio::deadline_timer>(*io_service);

	timer_Trush = std::make_shared<boost::asio::deadline_timer>(*io_service);

	oSlabMessager = std::make_shared<TcpMessager>();
}

SlabFileManager::~SlabFileManager() {
	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::~SlabFileManager");
}

/*
 * 写入数据过程:
 * 先检查本地内存是否有数据，如果本地内存有数据，检查本地内存数据版本和元数据节点的数据版本是否一致，如果一致，直接写入新数据，
 * 通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
 * 如果本地内存无数据，需要先读取数据到本地内存，然后写入新数据，通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
 *
 * 写入数据，会先清除状态缓存
 *
 * 往相同文件同时写入无法保证数据准确性
 *
 * 由于多线程异步并发环境下，存在一个情况，多个请求同时来请求相同文件的相同区域数据，如何避免多次读取后端存储数据？？？？
 * 一种解决方案，在 SlabBlock 里面 加锁单例任务进行回调读取，其他读取任务等待
 */

bool SlabFileManager::Write(const std::string & filename, off_t offset, const std::string & data_to_write,
		bool write_async, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientWriteResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	bool bMemoryFile = oBackendManager->IsMemory(filename);
	if (bMemoryFile == false) {
		if (oBackendManager->IsReadOnly(filename) == true) {
			this->ResponseEcho(CacheAction::caClientWriteResp, - EROFS, strerror(EROFS), conn);
			return true;
		}
	}

	size_t size = data_to_write.size();
	std::vector<uint32_t> BlockOffsetIds;
	if (size == 0 || CalcOffsetBlocks(offset, size, BlockOffsetIds) == 0) {
		this->ResponseEcho(CacheAction::caClientWriteResp, tsSuccess, "", conn);
		return true;
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get_or_create(filename, oSlabFile, 0, [ this , self, filename, bMemoryFile ]( ) {
//		oSlabFileNames.push_back( filename );
			return std::make_shared<SlabFile>( this, oSlabFactory, filename , bMemoryFile );
		});

	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caClientWriteResp, - EIO, strerror(EIO), conn);
		return true;
	}

	/**
	 * 更新访问时间
	 */
	oSlabFile->Update();
	/**
	 * 写入数据，清除状态缓存
	 */
	oSlabFile->ClearAttr();
	oSlabFile->ClearMeta();

	std::shared_ptr<SlabChainWriter> oSlabChainWriter = std::make_shared<SlabChainWriter>(self, oServerData, oSlabFile,
			conn, offset, data_to_write, write_async, BlockOffsetIds);

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabGetMeta);
	//获取所有块在元数据中的版本信息，存在一个风险，取回版本后，数据还保存完，元数据端被别人改了

	size_t nBlocks = BlockOffsetIds.size();

	int32_t uuid = oSlabFile->GetUuid();
	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->output->write_uint32(nBlocks);
	message->output->write_uint16(slab_port); /* 本地服务端口 */

	for (uint32_t block_offset_id : BlockOffsetIds) {
		message->output->write_uint32(block_offset_id);
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileManager::Write: " << filename << ", offset: " << offset << ", size: " << data_to_write.size());

	message->callback =
			[ this, self, conn, data_to_write, nBlocks, filename, oSlabChainWriter, oSlabFile]( std::shared_ptr<stringbuffer> input,
					const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

				//网络故障处理，当发送不接受任务后，服务端会关闭连接
				if(ec) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Write: " << filename << ", Error: " << ec.message() << ", " << meta_addr << ":" << meta_port );

					if(oServerData->enable_offline == true ) {
						LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Write, Enter offline mode: " << filename );
						/**
						 * 切换到 offline 模式
						 */
						oSlabChainWriter->WriteAsync( true );
						return;
					}

					this->ResponseEcho( CacheAction::caClientWriteResp, - EIO , ec.message(), conn);
					return;
				}

				int8_t action;
				uint32_t nBlocksResp;
				int32_t iMetaUuid;

				if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false
						|| input->read_uint32( nBlocksResp ) == false || action != CacheAction::caSlabGetMetaResp) {

					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Write: " << filename << ", Error: Invalid response");
					this->ResponseEcho(CacheAction::caClientWriteResp, - EIO , strerror(EIO) , conn);

					return;
				}

				/**
				 * 检查文件是否有效
				 */
				this-> CheckFileUuid(iMetaUuid, oSlabFile);

				for( uint32_t idx = 0; idx < nBlocksResp; idx ++) {
					uint32_t block_offset_id;
					int32_t version;
					uint32_t nPeers;

					if ( input->read_uint32( block_offset_id ) == false || input->read_int32( version ) == false
							|| input->read_uint32( nPeers ) == false ) {

						LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Write: " << filename << ", Error: Invalid response");
						this->ResponseEcho(CacheAction::caClientReadResp, - EIO , strerror(EIO) , conn);

						return;
					}
					oSlabChainWriter->PutVersion( block_offset_id, version );

					/**
					 * 获取所有邻居节点信息，放入列表，然后逐步尝试读取
					 */
					for ( uint32_t idx = 0; idx < nPeers; idx ++ ) {
						std::string peer;
						if( input->read_str( peer ) == false ) {
							break;
						}

						const std::shared_ptr<SlabPeer> & oSlabPeer = std::make_shared< SlabPeer >( peer );
						if ( oSlabPeer->getPort() == 0) {
							continue;
						}
						oSlabChainWriter->PutPeer( block_offset_id, oSlabPeer);
					}
				}

				oSlabChainWriter->PutMetaToCache();

				/**
				 * 放入一个 任务chain 中进行处理，任务 chain 完成后往 conn 返回数据，同时 自己销毁
				 */
				oSlabChainWriter->WriteAsync( false );
			};

	this->PostMessage(message);
	return true;
}

/**
 * 读取数据过程:
 * 1、获取文件块的分布信息：如果本地文件对象缓存具有这次度取需要的所有块信息，直接使用缓存块分布信息，否则从元数据服务器获取
 * 2、从待度取块序列开头开始，先在本地内存找数据块，如果本地内存有数据，检查本地内存数据版本与元数据版本是否一致，如果不一致删除本地内存数据，如果一致直接使用;
 * 3、如果本地内存没有，利用在块信息中的位置分布情况和最新版本号，根据位置分布情况，去第一个邻居节点的内存找数据（不找其他节点，理论上第一节点效率最高），
 * 4、如果第一邻居节点是最新数据，获取该节点数据副本到本地内存，同时更新元数据副本信息; 如果其它节点不是最新数据，删除该节点数据，更新元数据相应信息;
 * 5、如果其它节点都无数据，从后台存储读取数据，放入本地内存，更新元数据数据信息。
 *
 * 如果是 readonly，则启用元数据缓存
 *
 * 由于多线程异步并发环境下，存在一个情况，多个请求同时来请求相同文件的相同区域数据，如何避免多次读取后端存储数据？？？？
 * 一种解决方案，在 SlabBlock 里面 加锁单例任务进行回调读取，其他读取任务等待
 *
 */
bool SlabFileManager::Read(const std::string & filename, int8_t readonly, off_t offset, size_t bytes_to_read,
		const std::shared_ptr<asio_server_tcp_connection> & conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientReadResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	bool bMemoryFile = oBackendManager->IsMemory(filename);

	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get_or_create(filename, oSlabFile, 0, [ this , self, filename, bMemoryFile ]( ) {
		return std::make_shared<SlabFile>(this, oSlabFactory, filename, bMemoryFile );
	});

	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caClientReadResp, - EIO, strerror(EIO), conn);
		return true;
	}

	if (oBackendManager->IsReadOnly(filename) == true) {
		readonly = 1;
	}

	/**
	 * 更新访问时间
	 */
	oSlabFile->Update();

	/**
	 * 不判断读取长度是否超过文件长度？？
	 */

	std::vector<uint32_t> BlockOffsetIds;
	if (bytes_to_read == 0 || CalcOffsetBlocks(offset, bytes_to_read, BlockOffsetIds) == 0) {
		this->ResponseEcho(CacheAction::caClientReadResp, tsSuccess, "", conn);
		return true;
	}

	//	LOGGER_TRACE(
	//			"#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", offset: " << offset << ", size: " << size);

	std::shared_ptr<SlabChainReader> oSlabChainReader = std::make_shared<SlabChainReader>(self, oServerData, oSlabFile,
			conn, offset, bytes_to_read, BlockOffsetIds);

	//只读模式 如果本地缓存具有所需的元数据，缓存有效时间：60s，直接读取数据
	if (readonly == 1 && oSlabChainReader->GetMetaFromCache() == true) {
		//		LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Read, Cached SlabMeta: " << filename);
		oSlabChainReader->ReadAsync(false);
		return true;
	}

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabGetMeta);
	//获取所有块在元数据中的版本信息，存在一个风险，取回版本后，数据还保存完，元数据端被别人改了

	size_t nBlocks = BlockOffsetIds.size();

	int32_t uuid = oSlabFile->GetUuid();
	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->output->write_uint32(nBlocks);
	message->output->write_uint16(slab_port); /* 本地服务端口 */

	for (uint32_t block_offset_id : BlockOffsetIds) {
		message->output->write_uint32(block_offset_id);
	}

	message->callback =
			[ this, self, conn, nBlocks, filename, oSlabChainReader, oSlabFile ]( std::shared_ptr<stringbuffer> input,
					const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

				if(ec) {
					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: " << ec.message() << ", " << meta_addr << ":" << meta_port );
					/**
					 * 到元数据服务器，网络故障处理逻辑
					 * 可能元数据服务器断线了
					 */
					if(oServerData->enable_offline == true ) {
						LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Read, Enter offline mode: " << filename );
						/**
						 * 切换到 offline 模式
						 */
						oSlabChainReader->ReadAsync( true );
						return;
					}

					this->ResponseEcho(CacheAction::caClientReadResp, - EIO , ec.message(), conn);
					return;
				}

				/**
				 * 正常网络通讯处理逻辑
				 */

				int8_t action;
				int32_t iMetaUuid;
				uint32_t nBlocksResp;

				if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false || input->read_uint32( nBlocksResp ) == false || action != CacheAction::caSlabGetMetaResp ) {
					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: Invalid response");
					this->ResponseEcho(CacheAction::caClientReadResp, - EIO , strerror(EIO) , conn);
					return;
				}

				/**
				 * 检查本地文件是否有效，比对 UUID
				 */
				this-> CheckFileUuid(iMetaUuid, oSlabFile );

				for( uint32_t idx = 0; idx < nBlocksResp; idx ++) {
					uint32_t block_offset_id;
					int32_t version;
					uint32_t nPeers;

					if ( input->read_uint32( block_offset_id ) == false || input->read_int32( version ) == false
							|| input->read_uint32( nPeers ) == false ) {
						LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: Invalid response");
						this->ResponseEcho(CacheAction::caClientReadResp, - EIO , strerror(EIO) , conn);
						return;
					}

					oSlabChainReader->PutVersion( block_offset_id, version );

					/**
					 * 获取所有邻居节点信息，放入列表，然后逐步尝试读取
					 */
					for ( uint32_t idx = 0; idx < nPeers; idx ++ ) {
						std::string peer;
						if( input->read_str( peer ) == false ) {
							break;
						}

						const std::shared_ptr<SlabPeer> & oSlabPeer = std::make_shared< SlabPeer >( peer );
						if ( oSlabPeer->getPort() == 0) {
							continue;
						}
						oSlabChainReader->PutPeer( block_offset_id, oSlabPeer);
					}
				}

				oSlabChainReader->PutMetaToCache();
				/**
				 * 放入一个 任务chain 中进行处理，任务 chain 完成后往 conn 返回数据，同时 自己销毁
				 */
				oSlabChainReader->ReadAsync(false);
			};

	this->PostMessage(message);
	return true;
}

/**
 * Read2 函数实现，读取单独一块数据，在client进行组装
 *
 */
bool SlabFileManager::Read2(const std::string & filename, int8_t readonly, int32_t offset_id,
		const std::shared_ptr<asio_server_tcp_connection> & conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientReadResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	bool bMemoryFile = oBackendManager->IsMemory(filename);

	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get_or_create(filename, oSlabFile, 0, [ this , self, filename, bMemoryFile ]( ) {
		return std::make_shared<SlabFile>(this, oSlabFactory, filename, bMemoryFile );
	});

	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caClientReadResp, - EIO, strerror(EIO), conn);
		return true;
	}

	if (oBackendManager->IsReadOnly(filename) == true) {
		readonly = 1;
	}

	/**
	 * 更新访问时间
	 */
	oSlabFile->Update();

	/**
	 * 不判断读取长度是否超过文件长度？？ 读取完整一块，或最后一块
	 */

	std::vector<uint32_t> BlockOffsetIds;
	BlockOffsetIds.push_back(offset_id);

	/**
	 * 标记 bytes_to_read 为 0，代表 Read2 操作，只读取一块返回，在 SlabChainReader 内部进行逻辑判断
	 */
	size_t bytes_to_read = 0;
	off_t offset = 0;

//////////////////////////////////////////
	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Read2: " << filename << ", offset_id: " << offset_id);

	std::shared_ptr<SlabChainReader> oSlabChainReader = std::make_shared<SlabChainReader>(self, oServerData, oSlabFile,
			conn, offset, bytes_to_read, BlockOffsetIds);

//只读模式 如果本地缓存具有所需的元数据，缓存有效时间：60s，直接读取数据
	if (readonly == 1 && oSlabChainReader->GetMetaFromCache() == true) {
		//		LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Read, Cached SlabMeta: " << filename);
		oSlabChainReader->ReadAsync(false);
		return true;
	}

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabGetMeta);
//获取所有块在元数据中的版本信息，存在一个风险，取回版本后，数据还保存完，元数据端被别人改了

	size_t nBlocks = BlockOffsetIds.size();

	int32_t uuid = oSlabFile->GetUuid();
	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->output->write_uint32(nBlocks);
	message->output->write_uint16(slab_port); /* 本地服务端口 */

	for (uint32_t block_offset_id : BlockOffsetIds) {
		message->output->write_uint32(block_offset_id);
	}

	message->callback =
			[ this, self, conn, nBlocks, filename, oSlabChainReader, oSlabFile ]( std::shared_ptr<stringbuffer> input,
					const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

				if(ec) {
					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: " << ec.message() << ", " << meta_addr << ":" << meta_port );
					/**
					 * 到元数据服务器，网络故障处理逻辑
					 * 可能元数据服务器断线了
					 */
					if(oServerData->enable_offline == true ) {
						LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Read, Enter offline mode: " << filename );
						/**
						 * 切换到 offline 模式
						 */
						oSlabChainReader->ReadAsync( true );
						return;
					}

					this->ResponseEcho(CacheAction::caClientReadResp, - EIO , ec.message(), conn);
					return;
				}

				/**
				 * 正常网络通讯处理逻辑
				 */

				int8_t action;
				int32_t iMetaUuid;
				uint32_t nBlocksResp;

				if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false || input->read_uint32( nBlocksResp ) == false || action != CacheAction::caSlabGetMetaResp ) {
					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: Invalid response");
					this->ResponseEcho(CacheAction::caClientReadResp, - EIO , strerror(EIO) , conn);
					return;
				}

				/**
				 * 检查本地文件是否有效，比对 UUID
				 */
				this-> CheckFileUuid(iMetaUuid, oSlabFile );

				for( uint32_t idx = 0; idx < nBlocksResp; idx ++) {
					uint32_t block_offset_id;
					int32_t version;
					uint32_t nPeers;

					if ( input->read_uint32( block_offset_id ) == false || input->read_int32( version ) == false
							|| input->read_uint32( nPeers ) == false ) {
						LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Read: " << filename << ", Error: Invalid response");
						this->ResponseEcho(CacheAction::caClientReadResp, - EIO , strerror(EIO) , conn);
						return;
					}

					oSlabChainReader->PutVersion( block_offset_id, version );

					/**
					 * 获取所有邻居节点信息，放入列表，然后逐步尝试读取
					 */
					for ( uint32_t idx = 0; idx < nPeers; idx ++ ) {
						std::string peer;
						if( input->read_str( peer ) == false ) {
							break;
						}

						const std::shared_ptr<SlabPeer> & oSlabPeer = std::make_shared< SlabPeer >( peer );
						if ( oSlabPeer->getPort() == 0) {
							continue;
						}

						oSlabChainReader->PutPeer( block_offset_id, oSlabPeer);
					}
				}

				oSlabChainReader->PutMetaToCache();
				/**
				 * 放入一个 任务chain 中进行处理，任务 chain 完成后往 conn 返回数据，同时 自己销毁
				 */
				oSlabChainReader->ReadAsync( false );
			};

	this->PostMessage(message);
	return true;
}

ResultType SlabFileManager::PeerReadSlab(const std::string& filename, int32_t iMetaUuid, uint32_t block_offset_id,
		int32_t mVersion, std::shared_ptr<stringbuffer> & output,
		const std::shared_ptr<asio_server_tcp_connection> & conn) {

	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get(filename, oSlabFile);

	output->write_int8(CacheAction::caSlabPeerReadResp);

	if (oSlabFile.get() == NULL) {
		output->write_int32(0); //无数据
		return ResultType::rtSuccess;
	}

	/**
	 * 更新访问时间
	 */
	oSlabFile->Update();
	int32_t uuid = oSlabFile->GetUuid();

	/**
	 * 当前文件已经在元数据上删除，又被别人新建了一个同名文件，本地内存数据需要删除
	 */
	if (uuid != iMetaUuid) {
		oSlabFile->SetUuid(iMetaUuid);

		oSlabFile->ClearAttr();
		oSlabFile->ClearMeta();
		oSlabFile->ClearBlocks();

		oBackendManager->Close(filename);

		output->write_int32(0); //无数据
		return ResultType::rtSuccess;
	}

	if (mVersion < 1) {
		//说明元数据上面没有该数据，本地内存就是有数据也不能用
		//两种情况下可能出现 1 数据被其他人删除了，2 重启元数据导致丢失
		//有可能 存储上面有 文件
		oSlabFile->RemoveBlock(block_offset_id);

		output->write_int32(0); //无数据
		return ResultType::rtSuccess;
	}

	std::shared_ptr<SlabBlock> oSlabBlock = oSlabFile->GetBlock(block_offset_id);

	if (oSlabBlock.get() == NULL) {
		output->write_int32(0); //无数据
		return ResultType::rtSuccess;
	}

	int32_t lVersion = oSlabBlock->GetVersion();

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileManager::PeerReadSlab, GotIt: " << filename << ": " << block_offset_id << ", Version: " << mVersion);

	if (lVersion > 0 && lVersion >= mVersion) { //本地版本大于等于对方，返回本地数据
		size_t size = oSlabBlock->GetUsedSize();

		std::string data;
		int bytes_readed = oSlabBlock->Read(0, size, data);

		if (bytes_readed > 0) {
			output->write_int32(1); //有数据
			output->write_str(data.c_str(), bytes_readed);

			//更新元数据版本到本地一致
			LOGGER_TRACE(
					"#" << __LINE__ << ", SlabFileManager::PeerReadSlab, Update Peer: " << filename << ":" << block_offset_id << ", " << mVersion << " --> " << lVersion);

			UpdateSlabMeta(block_offset_id, oSlabFile, oSlabBlock);

			return ResultType::rtSuccess;
		}
	}

	/**
	 * 由于本地版本低于远程，需要 释放内存资源，并回收
	 *
	 */
	LOGGER_INFO(
			"#" << __LINE__ << ", SlabFileManager::PeerReadSlab, DropIt: " << filename << ": " << block_offset_id << ", Version: " << lVersion);

	oSlabFile->RemoveBlock(block_offset_id);
	output->write_int32(0); //无数据

	return ResultType::rtSuccess;
}

bool SlabFileManager::Unlink(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection> & conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientUnlinkResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Unlink: " << filename);

	bool bMemoryFile = oBackendManager->IsMemory(filename);

	if (bMemoryFile == false) {
		if (oBackendManager->IsReadOnly(filename) == true) {
			this->ResponseEcho(CacheAction::caClientUnlinkResp, - EROFS, strerror(EROFS), conn);
			return true;
		}

		oBackendManager->Close(filename);

		int e_code = 0;
		std::string e_message;

		if (oBackendManager->Unlink(filename, e_code, e_message) == false) {
			if (e_code == ENOENT) {
				LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Unlink: " << filename << ", Error: " << e_message);
			} else {
				LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::Unlink: " << filename << ", Error: " << e_message);
			}
			this->ResponseEcho(CacheAction::caClientUnlinkResp, e_code == 0 ? tsFailed : -abs(e_code), e_message, conn);
			return true;
		}
	}

	oSlabFiles_m.erase(filename);

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caClientUnlink);
	message->output->write_str(filename);

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	message->callback =
			[ this, self, conn, filename ]( std::shared_ptr<stringbuffer> input, const boost::system::error_code & ec,
					std::shared_ptr<base_connection> conn1 ) {

				if(ec) { //网络故障处理，当发送不接受任务后，服务端会关闭连接
					LOGGER_WARN("#" << __LINE__ << ", SlabFileManager::Unlink: " << filename << ", Error: " << ec.message());

					if(oServerData->enable_offline == true ) {
						LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Unlink, Enter offline mode: " << filename );
						/**
						 * 切换到 offline 模式
						 */
						this->ResponseEcho(CacheAction::caClientUnlinkResp, tsSuccess , "" , conn);
						return;
					}

					this->ResponseEcho(CacheAction::caClientUnlinkResp, tsFailed, ec.message(), conn );
					return;
				}

				int8_t action;
				if ( input->read_int8( action) == false || action != CacheAction::caClientUnlinkResp ) {
					LOGGER_INFO(
							"#" << __LINE__ << ", SlabFileManager::Unlink: " << filename << ", Error: Invalid response");
					this->ResponseEcho(CacheAction::caClientUnlinkResp, tsFailed , FAILED_INVALID_RESPONSE , conn);
					return;
				}

				this->ResponseEcho(CacheAction::caClientUnlinkResp, tsSuccess , "" , conn);
			};

	oSlabMessager->PostMessage(message);
	return true;
}

bool SlabFileManager::RmDir(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientRmDirResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::RmDir: " << filename);

	bool bMemoryFile = oBackendManager->IsMemory(filename);
	if (bMemoryFile == false) {
		if (oBackendManager->IsReadOnly(filename) == true) {
			this->ResponseEcho(CacheAction::caClientRmDirResp, - EROFS, strerror(EROFS), conn);
			return true;
		}

		int e_code = 0;
		std::string e_message;
		if (oBackendManager->RmDir(filename, e_code, e_message) == false) {
			if (e_code == ENOENT) {
				LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::RmDir: " << filename << ", Error: " << e_message);
			} else {
				LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::RmDir: " << filename << ", Error: " << e_message);
			}
			this->ResponseEcho(CacheAction::caClientRmDirResp, e_code == 0 ? tsFailed : -abs(e_code), e_message, conn);
			return true;
		}
	}

	this->ResponseEcho(CacheAction::caClientRmDirResp, tsSuccess, "", conn);
	return true;
}

bool SlabFileManager::MkDir(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientMkDirResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::MkDir: " << filename);

	bool bMemoryFile = oBackendManager->IsMemory(filename);
	if (bMemoryFile == false) {
		if (oBackendManager->IsReadOnly(filename) == true) {
			this->ResponseEcho(CacheAction::caClientMkDirResp, - EROFS, strerror(EROFS), conn);
			return true;
		}

		int e_code = 0;
		std::string e_message;
		if (oBackendManager->MkDir(filename, e_code, e_message) == false) {
			if (e_code == ENOENT) {
				LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::MkDir: " << filename << ", Error: " << e_message);
			} else {
				LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::MkDir: " << filename << ", Error: " << e_message);
			}
			this->ResponseEcho(CacheAction::caClientMkDirResp, e_code == 0 ? tsFailed : -abs(e_code), e_message, conn);
			return true;
		}
	}

	this->ResponseEcho(CacheAction::caClientMkDirResp, tsSuccess, "", conn);
	return true;
}

/**
 * 获取文件的属性：mtime & size，由于文件可能被其他机器修改，导致长度等变化，优先从本地缓存获取，如无
 * 针对相同文件采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
 * 对于 持久文件 直接从 后端存储读取，对于 内存文件 从服务器上获取
 *
 * 如果文件不存在，也需要缓存一下状态
 *
 */
bool SlabFileManager::GetAttr(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientGetAttrResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::GetAttr: " << filename);

	bool bMemoryFile = oBackendManager->IsMemory(filename);
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get_or_create(filename, oSlabFile, 0, [ this , self, filename, bMemoryFile]( ) {
		return std::make_shared<SlabFile>( this, oSlabFactory, filename, bMemoryFile );
	});

	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caClientGetAttrResp, - EIO, strerror(EIO), conn);
		return true;
	}

	oSlabFile->Update();

	this->EnsureGetAttr(oSlabFile,
			[this, self, oSlabFile, filename , conn ]( time_t stat_mtime, off_t stat_size, int e_code, const std::string & e_message ) {

				if ( e_code < 0 ) {
					if ( oSlabFile->StatEmpty() == true ) {
						LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::GetAttr, NotExist, DropIt: " << filename);
						oSlabFiles_m.erase( filename );
					}
					this->ResponseEcho(CacheAction::caClientGetAttrResp, e_code, e_message, conn);
					return;
				}

				if ( stat_mtime < 0 ) {
					this->ResponseEcho(CacheAction::caClientGetAttrResp, stat_mtime, strerror( abs(stat_mtime ) ) , conn);
					return;
				}

				struct FileStat fst;
				fst.mtime = stat_mtime;
				fst.size = stat_size;

				std::string s;
				s.assign((const char *) &fst, sizeof(struct FileStat));

				this->ResponseEcho(CacheAction::caClientGetAttrResp, tsSuccess, s, conn);
			});

	return true;
}

bool SlabFileManager::Truncate(const std::string& filename, off_t newsize,
		const std::shared_ptr<asio_server_tcp_connection>& conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientTruncateResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Truncate: " << filename);
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	bool bMemoryFile = oBackendManager->IsMemory(filename);
	if (bMemoryFile == true) {
		//内存文件暂时未实现
		return false;
	}

	if (oBackendManager->IsReadOnly(filename) == true) {
		this->ResponseEcho(CacheAction::caClientMkDirResp, - EROFS, strerror(EROFS), conn);
		return true;
	}

//是本地文件
	int e_code = 0;
	std::string e_message;
	if (oBackendManager->Truncate(filename, newsize, e_code, e_message) == false) {
		LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::Truncate: " << filename << ", Error: " << e_message);
		this->ResponseEcho(CacheAction::caClientTruncateResp, e_code == 0 ? tsFailed : -abs(e_code),
				"Failed: " + e_message, conn);
		return true;
	}

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caClientTruncate);
	message->output->write_str(filename);
	message->output->write_int64(newsize);

	oSlabFiles_m.erase(filename); //简化处理，删除本地缓存，删除服务器缓存
	message->callback =
			[ this, self, conn, filename ]( std::shared_ptr<stringbuffer> input, const boost::system::error_code & ec,
					std::shared_ptr<base_connection> conn1 ) {

				if(ec) { //网络故障处理，当发送不接受任务后，服务端会关闭连接
					LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Truncate: " << filename << ", Error: " << ec.message());

					if(oServerData->enable_offline == true ) {
						LOGGER_WARN( "#" << __LINE__ << ", SlabFileManager::Truncate, Enter offline mode: " << filename );
						/**
						 * 切换到 offline 模式
						 */
						this->ResponseEcho(CacheAction::caClientTruncateResp, tsSuccess , "" , conn);
						return;
					}

					this->ResponseEcho(CacheAction::caClientTruncateResp, tsFailed, ec.message(), conn );
					return;
				}

				int8_t action;
				if ( input->read_int8(action) == false || action != CacheAction::caClientTruncateResp ) {
					LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::Truncate: " << filename << ", Error: Invalid response" );
					this->ResponseEcho(CacheAction::caClientTruncateResp, tsFailed , FAILED_INVALID_RESPONSE , conn);
					return;
				}

				this->ResponseEcho(CacheAction::caClientTruncateResp, tsSuccess , "" , conn);
			};

	oSlabMessager->PostMessage(message);
	return true;
}

bool SlabFileManager::Close(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection>& conn) {
	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caSuccess, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	if (oBackendManager->IsMemory(filename) == true) {
		oSlabFiles_m.erase(filename);
		this->ResponseEcho(CacheAction::caSuccess, tsSuccess, "", conn);
		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Close: " << filename);
	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get(filename, oSlabFile);

	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caSuccess, tsSuccess, "", conn);
		return true;
	}

	if (oBackendManager->IsReadOnly(filename) == true) {
		oSlabFiles_m.erase(filename);
		oBackendManager->Close(filename);
		this->ResponseEcho(CacheAction::caSuccess, tsSuccess, "", conn);
		return true;
	}

	auto self = this->shared_from_this();
	oSlabFile->FlushDirty([ this, self, conn, filename ]( ssize_t state, const std::string & message ) {
		oSlabFiles_m.erase(filename);
		oBackendManager->Close(filename);
		this->ResponseEcho(CacheAction::caSuccess, state, message , conn );
	});
	return true;
}

bool SlabFileManager::Flush(const std::string& filename, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	if (oBackendManager->IsValid(filename) == false) {
		this->ResponseEcho(CacheAction::caClientFlushResp, - EINVAL, strerror(EINVAL), conn);
		return true;
	}

	if (oBackendManager->IsMemory(filename) == true) {
		this->ResponseEcho(CacheAction::caClientFlushResp, tsSuccess, "", conn);
		return true;
	}

	if (oBackendManager->IsReadOnly(filename) == true) {
		this->ResponseEcho(CacheAction::caClientFlushResp, - EROFS, strerror(EROFS), conn);
		return true;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::Flush: " << filename);
	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get(filename, oSlabFile);
	if (oSlabFile.get() == NULL) {
		this->ResponseEcho(CacheAction::caClientFlushResp, tsSuccess, "", conn);
		return true;
	}

	auto self = this->shared_from_this();

	oSlabFile->CancelLazyFlush();

	oSlabFile->FlushDirty([ this, self, conn ]( ssize_t state, const std::string & message ) {
		this->ResponseEcho(CacheAction::caClientFlushResp, state, message , conn );
	});
	return true;
}

/**
 * 修改文件的元数据：version, mtime & size
 */
void SlabFileManager::UpdateSlabMeta(uint32_t block_offset_id, const std::shared_ptr<SlabFile> & oSlabFile,
		const std::shared_ptr<SlabBlock> & oSlabBlock) {

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabPutMeta);

	int32_t uuid = oSlabFile->GetUuid();
	std::string filename = oSlabFile->GetFilename();

	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->output->write_uint32(block_offset_id);
	message->output->write_int32(oSlabBlock->GetVersion());

	message->output->write_uint16(slab_port); /* 本地服务端口 */

	message->output->write_int8(0); //不覆盖节点信息

	time_t mtime = oSlabFile->GetStatMtime();
	message->output->write_int64(mtime);

	off_t msize = oSlabFile->GetStatSize();
	message->output->write_int64(msize);

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileManager::UpdateSlabMeta: " << oSlabFile->GetFilename() << ", BlockId: " << block_offset_id << ", Version: " << oSlabBlock->GetVersion()
//			<< ", mtime:" << mtime << ", size: " << size
			);

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	message->callback = [ this, self, oSlabFile, filename ]( std::shared_ptr<stringbuffer> input,
			const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {
		//网络故障处理，当发送不接受任务后，服务端会关闭连接

			if(ec) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::UpdateSlabMeta: " << ec.message() );
				return;
			}

			int8_t action;
			int32_t iMetaUuid;
			int32_t state;

			if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false || input->read_int32( state ) == false ||
					action != CacheAction::caSlabPutMetaResp ) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::UpdateSlabMeta: " << filename << ", Error: Invalid response");
				return;
			}

			this->CheckFileUuid( iMetaUuid, oSlabFile );
		};

	this->PostMessage(message);
}

/**
 * 修改文件的属性：mtime & size
 */
void SlabFileManager::UpdateSlabAttr(const std::shared_ptr<SlabFile>& oSlabFile, time_t stat_mtime, off_t newsize,
		bool trunc_it) {

	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabPutAttr);

	int32_t uuid = oSlabFile->GetUuid();
	std::string filename = oSlabFile->GetFilename();

	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->output->write_int64(stat_mtime);
	message->output->write_int64(newsize);

	message->output->write_int8(trunc_it == true ? 1 : 0);

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileManager::UpdateSlabAttr: " << oSlabFile->GetFilename() << ", mtime:" << stat_mtime << ", size: " << newsize);

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	message->callback = [ this, self, oSlabFile, filename ]( std::shared_ptr<stringbuffer> input,
			const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {
		//网络故障处理，当发送不接受任务后，服务端会关闭连接
			if(ec) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::UpdateSlabAttr: " << oSlabFile->GetFilename() << ", Error: " << ec.message() );
				return;
			}

			int8_t action;
			int32_t iMetaUuid;
			int32_t state;

			if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false || input->read_int32( state ) == false ||
					action != CacheAction::caSlabPutAttrResp ) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::UpdateSlabAttr: " << filename << ", Error: Invalid response");
				return;
			}

			this-> CheckFileUuid( iMetaUuid, oSlabFile );
		};

	this->PostMessage(message);
}

/**
 * 元数据服务主动来检查文件的块是否有效，如果无效，本地删除同时反馈元数据进行删除
 */

ResultType SlabFileManager::CheckItSlab(const std::shared_ptr<stringbuffer>& input,
		std::shared_ptr<stringbuffer>& output, const std::shared_ptr<asio_server_tcp_connection>& conn) {

	int32_t iMetaUuid;
	std::string filename;
	uint32_t nBlocks = 0;

	if (input->read_int32(iMetaUuid) == false || input->read_str(filename) == false
			|| input->read_uint32(nBlocks) == false) {
		return ResultType::rtFailed;
	}

	output->write_int8(CacheAction::caMasterCheckItResp);

	/**
	 * 如果本地没有该文件，返回 0
	 */
//	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::CheckItSlab: " << filename);
	std::shared_ptr<SlabFile> oSlabFile;
	oSlabFiles_m.get(filename, oSlabFile);

	if (oSlabFile.get() == NULL) {
		output->write_int32(0);
		return ResultType::rtSuccess;
	}

	/**
	 * 如果 uuid 不一致，本地作废
	 */
	if (oSlabFile->GetUuid() != iMetaUuid) {
		oBackendManager->Close(filename);
		oSlabFiles_m.erase(filename);

		output->write_int32(0);
		return ResultType::rtSuccess;
	}

	output->write_uint32(nBlocks);
	for (uint32_t i = 0; i < nBlocks; i++) {
		uint32_t block_id;
		if (input->read_uint32(block_id) == false) {
			break;
		}
		/**
		 * 如果本地缓存没有该块，返回元数据服务器进行删除
		 */
		std::shared_ptr<SlabBlock> oSlabBlock = oSlabFile->GetBlock(block_id);

		if (oSlabBlock.get() == NULL) {
			oSlabFile->RemoveBlock(block_id);
			output->write_uint32(block_id);
		}
	}

	return ResultType::rtSuccess;
}

void SlabFileManager::start() {
	ReportStatus();
	TraceTrushes(1.0);
}

void SlabFileManager::stop() {
	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::stop")

	oSlabFactory->stop();

	boost::system::error_code ec;
	timer_Status->cancel(ec);
	timer_Trush->cancel(ec);
}

void SlabFileManager::TraceTrushes(float factor) {
	boost::system::error_code ec;
	timer_Trush->cancel(ec);

	size_t nFiles = oSlabFiles_m.size();

	float base_time = 32 * 1000;
	if (nFiles > 6400) {
		base_time = 1 * 1000;
	} else if (nFiles > 3200) {
		base_time = 2 * 1000;
	} else if (nFiles > 1600) {
		base_time = 4 * 1000;
	} else if (nFiles > 800) {
		base_time = 8 * 1000;
	} else if (nFiles > 400) {
		base_time = 16 * 1000;
	}

	base_time /= 2;

	base_time *= factor;
	if (base_time < 10) {
		base_time = 10;
	}

//	base_time = 50;

	timer_Trush->expires_from_now(boost::posix_time::milliseconds(static_cast<int>(base_time)));

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFileManager::TraceTrushes, " << nFiles << " files, Check Interval: " << base_time << " ms");

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	timer_Trush->async_wait([ this, self, nFiles ](const boost::system::error_code &ec) {
		if(ec) {
			return;
		}

		int state = oSlabFiles_m.gc( [ this, self ]( const std::shared_ptr< SlabFile > & gSlabFile ) {

					/**
					 * 如果是刚修改后的数据，暂时不做处理
					 */
					if( gSlabFile->IsTimeout( TIMER_TRUSH_TTL ) == false ) {
						return true;
					}

					if ( gSlabFile->TraceTrushes( ) == 0 ) {

						LOGGER_TRACE( "#" << __LINE__ << ", SlabFileManager::TraceTrushes, Zero blocks, EraseIt: "
								<< gSlabFile->GetFilename());
						return false;

					}

					return true;
				});

		float fa = state > 0? 0.1 : 1.0;
		/**
		 * 发现垃圾，加速检查下一个
		 */

		TraceTrushes( fa );
	});
}

void SlabFileManager::ReportStatus() {
	boost::system::error_code ec;
	timer_Status->cancel(ec);

	timer_Status->expires_from_now(boost::posix_time::seconds(1));
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	timer_Status->async_wait([ this, self ](const boost::system::error_code &ec) {
		if(ec) {
			return;
		}

		std::shared_ptr<TcpMessage> message = NewMetaMessage( CacheAction::caSlabStatus );

		message->output->write_uint16(slab_port); /* 本地服务端口 */

		message->output->write_uint64( oNetworkSpeed.GetUsedTmUsec() ); /* 网络通讯延迟 */
		message->output->write_uint32( SIZEOFBLOCK ); /* 状态 缓存块尺寸 */

		message->output->write_uint64(oSlabFactory->GetReadMemBlocks());/*状态 读取块数 */
		message->output->write_uint64(oSlabFactory->GetWriteMemBlocks());/*状态 写入块数 */
		message->output->write_uint64(oSlabFactory->GetFreeMemBlocks());/*状态 空闲块数 */

		message->output->write_uint64(oSlabFactory->GetReadSwapBlocks());/*状态 读取块数 */
		message->output->write_uint64(oSlabFactory->GetWriteSwapBlocks());/*状态 写入块数 */
		message->output->write_uint64(oSlabFactory->GetFreeSwapBlocks());/*状态 空闲块数 */

		size_t nFiles = oSlabFiles_m.size();
		message->output->write_uint64( nFiles ); /* 总文件块数 */

		message->callback = [ this, self ]( std::shared_ptr<stringbuffer> input, const boost::system::error_code & ec,
				std::shared_ptr<base_connection> conn ) {

			if(ec) { //网络故障处理，当发送不接受任务后，服务端会关闭连接
//				LOGGER_TRACE( "#" << __LINE__ << ", SlabFileManager::ReportStatus: " << ec.message() << ", retry it later");
				ReportStatus();
				return;
			}
			oNetworkSpeed.Done();

			int8_t action;
			if ( input->read_int8( action) == false ) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::ReportStatus: Invalid response, retry it later");
				ReportStatus();
				return;
			}

			if (action == CacheAction::caSlabStatusResp) {
				ReportStatus();
				return;
			}

			LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::ReportStatus: Invalid response, retry it later");
			ReportStatus();
		};

		oNetworkSpeed.Start(); //开始计算时间
//		LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::ReportStatus: " << slab_port);
		oSlabMessager->PostMessage( message );
	});
}

void SlabFileManager::CheckFileUuid(int32_t iMetaUuid, const std::shared_ptr<SlabFile>& oSlabFile) {
	/**
	 * 如果元数据端没有当前文件，则 iMetaUuid == 0 同时 nBlocksResp == 0，stat_mtime == 0, stat_size == 0
	 * 如果元数据端文件被其他人删除，后其他人又建立了一个同名文件，则uuid会不同了，
	 */

	int32_t uuid = oSlabFile->GetUuid();

	if ((iMetaUuid == 0) || (uuid != iMetaUuid)) {
		//本文件的 内存数据清空，重构一个
		oSlabFile->ClearMeta();
		oSlabFile->ClearBlocks();

		std::string filename = oSlabFile->GetFilename();
		oBackendManager->Close(filename);

		if (iMetaUuid > 0) { // 服务端是另一个文件，将自己UUID保持与元数据端一致
			LOGGER_INFO("#" << __LINE__ << ", SlabFileManager::CheckFileUuid, NotSame, DropIt: " << filename);
			oSlabFile->SetUuid(iMetaUuid);
			oSlabFile->ClearAttr();
		}
	}
}

int SlabFileManager::CalcOffsetBlocks(off_t offset, size_t size, std::vector<uint32_t> & BlockOffsetIds) {
	if (size <= 0) {
		return 0;
	}

	if (offset < 0) {
		offset = 0;
	}

	off_t uend = offset + size;
	off_t ubeg = (offset / SIZEOFBLOCK) * SIZEOFBLOCK;

	while (ubeg < uend) {
		uint32_t block_id = ubeg / SIZEOFBLOCK;
		BlockOffsetIds.push_back(block_id);
		ubeg += SIZEOFBLOCK;
	}
	return BlockOffsetIds.size();
}

/**
 * 获取文件的属性：mtime & size，由于文件可能被其他机器修改，导致长度等变化，优先从本地缓存获取，如无
 * 针对相同文件采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
 * 对于 持久文件 直接从 后端存储读取，对于 内存文件 从服务器上获取
 */
void SlabFileManager::EnsureGetAttr(const std::shared_ptr<SlabFile>& oSlabFile, const GetAttrCallback & callback) {
	if (oSlabFile.get() == NULL) {
		callback(0, 0, - EIO, strerror(EIO));
		return;
	}

	if (oSlabFile->StatValid(oServerData->stat_ttl)) { //缓存属性存在且可用
		LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::EnsureGetAttr, from Cache: " << oSlabFile->GetFilename());
		callback(oSlabFile->GetStatMtime(), oSlabFile->GetStatSize(), tsSuccess, "");
		return;
	}

	std::string filename = oSlabFile->GetFilename();
	bool bMemoryFile = oSlabFile->IsMemoryFile();
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::EnsureGetAttr: " << oSlabFile->GetFilename());

	/**
	 * 对于有 backend 的文件，直接从 backend 获取，同时在本地缓存
	 */
	if (bMemoryFile == false) {
		const std::shared_ptr<mtsafe::CallBarrier<bool>> & oSlabBarrier = oSlabFile->GetBarrier();

		const auto & fSyncCallback =
				[ this, self, filename, oSlabFile, callback]() {

					if (oSlabFile->StatValid(oServerData->stat_ttl) == true ) {
						return true;
					}

					/**
					 * 缓存属性不可用，从后端重新加载
					 */
					int e_code;
					std::string e_message;
					struct stat st;

					if (oBackendManager->GetAttr(filename, &st, e_code, e_message) == false) {
						oSlabFile->ClearAttr();
						if ( e_code == ENOENT ) {
							/**
							 缓存文件不存在信息
							 */
							oSlabFile->SetStatMtime( -ENOENT );
							oSlabFile->StatUpdate();
							callback( -ENOENT , 0, tsSuccess, "");
							return false;
						}

						LOGGER_INFO(
								"#" << __LINE__ << ", SlabFileManager::EnsureGetAttr, Backend GetAttr: " << filename << ", Error: " << e_message);

						callback( 0 , 0, -abs(e_code), e_message);
						return false;
					}

					LOGGER_TRACE(
							"#" << __LINE__ << ", SlabFileManager::EnsureGetAttr, Backend GetAttr: " << filename << ", mtime: " << st.st_mtim.tv_sec << ", size: " << st.st_size);

					oSlabFile->SetStatMtime(st.st_mtim.tv_sec);
					oSlabFile->SetStatSize(st.st_size);
					oSlabFile->StatUpdate();

					/**
					 * 更新服务器上文件长度
					 */
					UpdateSlabAttr(oSlabFile, st.st_mtim.tv_sec, st.st_size, false);
					return true;
				};

		/**
		 * 获取文件属性，针对相同文件采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
		 * oBackendManager->GetAttr 失败，已经返回错误信息到client，这里只需要返回 true
		 */
		if (oSlabBarrier->CallSync(fSyncCallback) == false) {
			return;
		}

		callback(oSlabFile->GetStatMtime(), oSlabFile->GetStatSize(), tsSuccess, "");
		return;
	}

	/**
	 * 内存临时文件，从元数据上获取，如果没有，代表是新的，重建
	 */
	std::shared_ptr<TcpMessage> message = NewMetaMessage(CacheAction::caSlabGetAttr); //获取所有块在元数据中的版本信息，存在一个风险，取回版本后，数据还保存完，元数据端被别人改了

	int32_t uuid = oSlabFile->GetUuid();
	message->output->write_int32(uuid);
	message->output->write_str(filename);

	message->callback = [ this, self, uuid , filename, oSlabFile, callback ]( std::shared_ptr<stringbuffer> input,
			const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

		//网络故障处理，当发送不接受任务后，服务端会关闭连接
			if(ec) {
				LOGGER_INFO(
						"#" << __LINE__ << ", SlabFileManager::EnsureGetAttr: " << filename << ", Error: " << ec.message());
				callback(0, 0, - EIO , strerror(EIO) );
				return;
			}

			int8_t action;
			int32_t iMetaUuid;

			time_t stat_mtime;
			off_t stat_size;

			if ( input->read_int8( action ) == false || input->read_int32( iMetaUuid ) == false || input->read_int64( stat_mtime ) == false ||
					input->read_int64( stat_size ) == false || action != CacheAction::caSlabGetAttrResp ) {
				LOGGER_INFO( "#" << __LINE__ << ", SlabFileManager::EnsureGetAttr: " << filename << ", Error: Invalid response");
				callback(0, 0, - EIO , strerror(EIO) );
				return;
			}

			/**
			 * 如果元数据端没有当前文件，则 iMetaUuid == 0 同时 nBlocksResp == 0
			 * 如果元数据端文件被其他人删除，后其他人又建立了一个同名文件，则uuid会不同了，
			 */
			if( (iMetaUuid == 0) || ( uuid != iMetaUuid )) {
//本文件的 内存数据清空，重构一个
				if (iMetaUuid > 0) {
					oSlabFile->SetUuid( iMetaUuid );
				}

				oSlabFile->ClearMeta();
				oSlabFile->ClearBlocks();
				oBackendManager->Close( filename );
			}

			oSlabFile->SetStatMtime( stat_mtime );
			oSlabFile->SetStatSize( stat_size );
			oSlabFile->StatUpdate();

			callback(stat_mtime, stat_size, tsSuccess, "");
		};

	this->PostMessage(message);
}

void SlabFileManager::ResponseEcho(int8_t action, ssize_t bytes_state, const std::string & message_or_data,
		const std::shared_ptr<asio_server_tcp_connection>& conn) {

	if (conn == nullptr) {
		return;
	}

	std::shared_ptr<stringbuffer> output = std::make_shared<stringbuffer>();

	output->write_int8(action);
	output->write_int32(bytes_state);
	output->write_str(message_or_data);

	conn->async_write(output);
}

std::shared_ptr<TcpMessage> SlabFileManager::NewMetaMessage(int action) {
	std::shared_ptr<TcpMessage> message = std::make_shared<TcpMessage>();

	message->host = meta_addr;
	message->port = meta_port;

	message->output->write_int8(action);

	return message;
}

void SlabFileManager::PostMessage(const std::shared_ptr<TcpMessage> & message) {
	oSlabMessager->PostMessage(message);
}

