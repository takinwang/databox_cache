/*
 * SlabChainOp.cpp
 *
 *  Created on: Nov 23, 2018
 *      Author: root
 */

#include <databox/cpl_memdog.hpp>
//INIT_IG(SlabChainOp_watchdog, "SlabChainOp");

SlabChainOp::SlabChainOp(const std::shared_ptr<SlabFileManager> & oSlabFileManager_,
		const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile> & oSlabFile_,
		const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_,
		const std::vector<uint32_t>& oBlockOffsetIds_) :
		oSlabFileManager(oSlabFileManager_), oServerdata(serverdata_), oSlabFile(oSlabFile_), conn(conn_), offset(
				offset_) {

//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainOp::SlabChainOp" << ", " << (long) this);
//	INC_IG(SlabChainOp_watchdog);

	oBackendManager = oSlabFileManager_->oBackendManager;
	filename = oSlabFile_->GetFilename();

	io_strand = AsyncIOService::getInstance()->getStrand();

	bool bFirst = false;
	for (uint32_t block_offset_id : oBlockOffsetIds_) {

		if (bFirst == false) {
			iFisrtBlockOffsetId = block_offset_id;
			bFirst = true;
		}

		iLastBlockOffsetId = block_offset_id;
		oBlockOffsetIds.push_back(block_offset_id);
	}
}

SlabChainOp::~SlabChainOp() {
//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainOp::~SlabChainOp" << ", " << (long) this);
//	DEC_IG(SlabChainOp_watchdog);
}

/**
 * 尝试从邻居读取数据失败
 * 删除一块数据缓存邻居信息，这些邻居不可用，直接从底部存储读取数据
 */
void SlabChainOp::RemoveSlabPeer(uint32_t block_offset_id, const std::string & peer) {
	oSlabOffsetMetas[block_offset_id].oSlabPeers.clear();
	oSlabFile->RemoveMeta(block_offset_id);
}

/**
 * 从邻居或后端加载数据，然后创建内存块进行保存
 * 相同块可能同步进入，是否需要加锁？
 */
void SlabChainOp::LoadDataToSlab(const char * buffer, size_t buffer_size, uint32_t block_offset_id, int32_t mVersion,
		const SlabCallback& callback) {

	if (buffer_size == 0) { //没有数据了
		callback(tsSuccess, "", oNullSlabBlock);
		return;
	}

	std::shared_ptr < SlabBlock > oSlabBlock = oSlabFileManager->oSlabFactory->New();
	if (oSlabBlock.get() == NULL) {
		/**
		 * 如果系统故障，导致没有缓存块
		 */

		LOGGER_TRACE("#" << __LINE__ << ", SlabChainOp::LoadDataToSlab, Failed: Out of Memory");
		callback(-ENOMEM, strerror(ENOMEM), oNullSlabBlock);
		return;
	}

	/**
	 * 保存数据到内存块
	 */
	oSlabBlock->WriteBlock(buffer, buffer_size, 0);
	oSlabBlock->SetVersion(mVersion < 1 ? 1 : mVersion);

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainOp::LoadDataToSlab, PutCache: " << filename << ", BlockId: "
					<< block_offset_id << ", Version: " << oSlabBlock->GetVersion());

	oSlabFile->AddBlock(block_offset_id, oSlabBlock);
	oSlabBlock->Commit();

	callback(tsSuccess, "", oSlabBlock);
}

/**
 * 异步方式从各 邻居节点 读取数据，如果都没有，调用后台 backend 读取数据
 */
void SlabChainOp::ReadOneSlabPeer(uint32_t block_offset_id, int32_t mVersion, const SlabCallback & callback,
		bool offline) {

	std::list < std::shared_ptr < SlabPeer >> &oSlabPeers = oSlabOffsetMetas[block_offset_id].oSlabPeers;
	if (offline == true || oSlabPeers.empty() == true) {
		/**
		 *所有邻居没有数据，从底部存储读取数据
		 *离线模式下网络故障，直接从后端读取
		 */
		ReadBackend(block_offset_id, mVersion, callback, offline);
		return;
	}

	std::shared_ptr < SlabPeer > oSlabPeer = oSlabPeers.front();
	oSlabPeers.pop_front();

	std::shared_ptr < TcpMessage > message = oSlabFileManager->NewMetaMessage(CacheAction::caSlabPeerRead);
//直接从邻居的内存读取数据，如果版本合格，返回数据

	message->host = oSlabPeer->getHost();
	message->port = oSlabPeer->getPort();

	message->output->write_int32(oSlabFile->GetUuid());
	message->output->write_str(filename);

	message->output->write_uint32(block_offset_id);

	message->output->write_int32(mVersion);

	oSlabPeers.clear(); // 不管成功与否，该块的所有邻居都 clear，因为自己已经有数据了

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainOp::ReadOneSlabPeer: " << filename << ", BlockId: " << block_offset_id
					<< ", Version: " << mVersion << ", " << message->host << ":" << message->port);

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	message->callback =
			[ this, self, oSlabPeer, block_offset_id , mVersion ,callback, offline ]( std::shared_ptr<stringbuffer> input,
					const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

				//网络故障处理，尝试其他邻居和底部存储
				if(ec) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabChainOp::ReadOneSlabPeer: " << filename << ", Error: " << ec.message() );
					RemoveSlabPeer( block_offset_id, oSlabPeer->getKey() );
					ReadBackend(block_offset_id, mVersion, callback, offline);
					return;
				}

				int8_t action;
				int32_t state;
				if ( input->read_int8( action ) == false || action != CacheAction::caSlabPeerReadResp || input->read_int32( state ) == false || state == 0 ) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabChainOp::ReadOneSlabPeer: " << filename << ", Error: Invalid response");
					RemoveSlabPeer( block_offset_id , oSlabPeer->getKey() );
					ReadBackend(block_offset_id, mVersion, callback, offline);
					return;
				}

				char * ptr = read_buffer.resize(SIZEOFBLOCK);
				if (ptr == NULL) {
					callback( -ENOMEM, strerror(ENOMEM), oNullSlabBlock);
					return;
				}

				size_t bytes_readed = 0;
				if( input->read_str( ptr, SIZEOFBLOCK, bytes_readed ) == false || bytes_readed == 0 ) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabChainOp::ReadOneSlabPeer: " << filename << ", Error: Invalid response");
					RemoveSlabPeer( block_offset_id, oSlabPeer->getKey() );
					ReadBackend(block_offset_id, mVersion, callback, offline);
					return;
				}

//				LOGGER_TRACE(
//						"#" << __LINE__ << ", SlabChainOp::ReadOneSlabPeer, GotIt: " << filename
//						<< ", BlockId: " << block_offset_id << ", Version: " << mVersion << ", from: " << oSlabPeer->getHost()
//						<< ":" << oSlabPeer->getPort());

				this->LoadDataToSlab( ptr, bytes_readed, block_offset_id, mVersion, callback );

				/**
				 * 从邻居那儿拿到数据了，保存到本地内存，同时通知元数据节点，这儿有数据
				 */
			};

	oSlabFileManager->PostMessage(message);
}

/**
 * 从底部存储中读取数据，如果是内存数据，直接返回客户端了
 * 如果是内存中数据，读取的 offset 和 size 之间存在空白区域，那么空白区域之后的数据会被忽略
 *
 * 由于多线程异步并发环境下，存在一个情况，多个请求同时来请求相同文件的相同区域数据，如何避免多次读取后端存储数据？？？？
 * 一种解决方案，在 SlabBlockHelper 里面 加锁单例任务进行回调读取，其他读取任务等待
 */
void SlabChainOp::ReadBackend(uint32_t block_offset_id, int32_t mVersion, const SlabCallback & callback, bool offline) {

	if (oBackendManager->IsMemory(filename) == true || bBackendMore == false) {
		callback(tsSuccess, "", oNullSlabBlock);
		return;
	}

	LOGGER_TRACE("#" << __LINE__ << ", SlabChainOp::ReadBackend: " << filename << ", BlockId: " << block_offset_id);

	std::shared_ptr < Backend > oBackend = oBackendManager->Open(filename, false);

	if (oBackend.get() == NULL) {
		callback(-ENOENT, strerror(ENOENT), oNullSlabBlock);
		// 文件名称错误，返回读取数据错误
		return;
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	/**
	 * 获取块对象的锁，针对相同块进行加锁读取，相同文件的相同块只会读取一次
	 */
	const std::shared_ptr<mtsafe::CallBarrier<bool> > & oSlabBarrier = oSlabFile->GetBarrier(block_offset_id);

	const auto & fSyncCallback =
			[ this, self, block_offset_id, mVersion, oBackend, offline, callback ]() {

				LOGGER_TRACE(
						"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync: " << filename << ", BlockId: " << block_offset_id
						<< ", Version: " << mVersion);

				std::shared_ptr< SlabBlock > oSlabBlock = oSlabFile->GetBlock(block_offset_id);

				/** 相同块按顺序进入，其他读取任务已经读取了数据，直接使用
				 */
				if (oSlabBlock.get() != NULL) {
					//如果本地内存有数据，检查本地内存数据版本与元数据版本是否一致，如果本地版本小于远程，删除本地内存数据，如果一致或大于，直接写入;
					int32_t lVersion = oSlabBlock->GetVersion();

					bool isvalid = false;
					if (lVersion > 0 && lVersion >= mVersion) {
						/* 情况下正常，本地版本 大于或等于，直接读取
						 */
						isvalid = true;
					}

					if (offline == true) {
						/**
						 * 离线模式下网络故障，不需要判断版本号
						 * 缓存块直接可用
						 */
						isvalid = true;
					}

					if (isvalid == true) {
						/*
						 * 缓存块直接可用
						 */
//						LOGGER_TRACE(
//								"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync, GotIt: " << filename << ", BlockId: " << block_offset_id
//								<< ", Version: " << mVersion);
						callback(tsSuccess, "", oSlabBlock);
						return true;
					}
				}

				uint64_t offset = block_offset_id * SIZEOFBLOCK;
				uint32_t size = SIZEOFBLOCK;

				char * ptr = read_buffer.resize(SIZEOFBLOCK);
				if (ptr == NULL) {
					callback( -ENOMEM, strerror(ENOMEM), oNullSlabBlock);
					return true;
				}

				int bytes_readed = oBackend->Read((void *) ptr, size, offset);
				if (bytes_readed < 0) {
					if ( oBackend->code != ENOENT ) {
						LOGGER_WARN(
								"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync: " << filename << ", BlockId: " << block_offset_id
								<< ", Error: " << oBackend->message);
					} else {
						LOGGER_TRACE(
								"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync: " << filename << ", BlockId: " << block_offset_id
								<< ", Error: " << oBackend->message);
					}

					//底部存储发生故障，应该不能继续了
					//来自写入数据前的读取数据请求
					//底部存储故障，有可能文件不存在或错误，写入忽略错误，继续写入，后续正式写入在说，读取则报错
					callback(-abs(oBackend->code), oBackend->message, oNullSlabBlock);
					return true;
				}

				if (bytes_readed > 0) {
					LOGGER_TRACE(
							"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync: " << filename << ", BlockId: " << block_offset_id
							<< ", " << bytes_readed << " bytes");
				}

				if (bytes_readed < SIZEOFBLOCK) { //读取不到一个完整块，不需要后取读取底部存储
					bBackendMore = false;
					LOGGER_TRACE(
							"#" << __LINE__ << ", SlabChainOp::ReadBackend, CallSync, No more backend data: " << filename << ", BlockId: "
							<< block_offset_id);
				}

				LoadDataToSlab(ptr, bytes_readed, block_offset_id, mVersion, callback);
				return true;
			};

	/**
	 * 针对相同文件的相同块采用加锁方式读取，避免相同文件多线程访问造成不必要访问后端
	 */
	oSlabBarrier->CallSync(fSyncCallback);
}

void SlabChainOp::PutVersion(uint32_t block_offset_id, int32_t version) {
	oSlabOffsetMetas[block_offset_id].version = version;
}

void SlabChainOp::PutPeer(uint32_t block_offset_id, const std::shared_ptr<SlabPeer> & oSlabPeer) {
	oSlabOffsetMetas[block_offset_id].oSlabPeers.push_back(oSlabPeer);
}

void SlabChainOp::PutMetaToCache() {
	for (auto iter = oSlabOffsetMetas.begin(); iter != oSlabOffsetMetas.end(); iter++) {
		oSlabFile->PutMeta(iter->first, iter->second, oServerdata->meta_ttl);
	}
}

bool SlabChainOp::GetMetaFromCache() {
	oSlabOffsetMetas.clear();
	for (auto iter = oBlockOffsetIds.begin(); iter != oBlockOffsetIds.end(); iter++) {
		uint32_t block_offset_id = *iter;

		std::shared_ptr < SlabMeta > oSlabMeta;
		if (oSlabFile->GetMeta(block_offset_id, oSlabMeta) == false) {
			return false;
		}

//		oSlabMeta->bReport = false; //缓存里面有了，说明本地已经,不需要汇报
		oSlabOffsetMetas[block_offset_id].CopyFrom(oSlabMeta.get());
	}
	return true;
}

#include "SlabChainReader.inc"
#include "SlabChainWriter.inc"

