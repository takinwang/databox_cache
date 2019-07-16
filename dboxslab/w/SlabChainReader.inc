#include <databox/cpl_memdog.hpp>
INIT_IG(SlabChainReader_watchdog, "SlabChainReader");

SlabChainReader::SlabChainReader(const std::shared_ptr<SlabFileManager>& oSlabFileManager_,
		const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile>& oSlabFile_,
		const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_, size_t bytes_to_read_,
		const std::vector<uint32_t>& oBlockOffsetIds_) :
		SlabChainOp::SlabChainOp(oSlabFileManager_, serverdata_, oSlabFile_, conn_, offset_, oBlockOffsetIds_), bytes_to_read(
				bytes_to_read_) {
//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainReader::SlabChainReader" << ", " << (long) this);
	INC_IG(SlabChainReader_watchdog);
}

SlabChainReader::~SlabChainReader() {
	DEC_IG(SlabChainReader_watchdog); //
//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainReader::~SlabChainReader" << ", " << (long) this);
}

/**
 * 异步读取数据，内部函数调用链
 * 离线模式下网络故障时候，offline = true
 */
void SlabChainReader::ReadAsync(bool offline) {
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	io_strand->post([ this, self, offline ]() {
		this->Read( offline );
	});
}

/**
 * 读取数据过程:
 * 先在本地内存找数据块，
 * 如果本地内存有数据，检查本地内存数据版本与元数据版本是否一致，如果本地版本小于远程，删除本地内存数据，如果一致或大于，直接写入;
 * 如果本地内存没有，在元数据节点寻找数据的位置分布情况和最新版本号，根据位置分布情况，依次去相应节点的内存找数据，
 * 如果是最新数据，获取该节点数据副本到本地内存，同时更新元数据副本信息; 如果其它节点不是最新数据，删除该节点数据，更新元数据相应信息;
 * 如果其它节点都无数据，从后台存储读取数据，放入本地内存，更新元数据数据信息。
 *
 * 离线模式下网络故障时候，offline = true
 */
void SlabChainReader::Read(bool offline) {
	if (oBlockOffsetIds.empty() == true || bBackendMore == false) { //数据全部读取完成
		oSlabFileManager->ResponseEcho(CacheAction::caClientReadResp, tsSuccess, rb_buffer.str(), conn);
		return;
	}

	uint32_t block_offset_id = oBlockOffsetIds.front();
	oBlockOffsetIds.pop_front();

	if (bytes_to_read == 0) {
		/**
		 * 来自 Read2 函数，一次读取完整一块
		 */
		oBlockOffsetIds.clear();
	}

	ReadOneSlab(block_offset_id, offline);
}

/**
 * 一次读取一块，链式调用
 * 离线模式下网络故障时候，offline = true
 *
 */
void SlabChainReader::ReadOneSlab(uint32_t block_offset_id, bool offline) {

	int32_t mVersion = 0;
	bool bReport = true;

	if (offline == false) {
		/*
		 * 正常网络下，使用版本号，以及判断是否需要汇报给元数据
		 */
		if (oSlabOffsetMetas.find(block_offset_id) != oSlabOffsetMetas.end()) {
			const SlabMeta & oSlabMeta = oSlabOffsetMetas[block_offset_id];
			mVersion = oSlabMeta.version;
			bReport = oSlabMeta.bReport;
		}
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	const auto & callback =
			[this, self, block_offset_id, mVersion, offline, bReport ] ( int state, const std::string & message,
					std::shared_ptr<SlabBlock> oSlabBlock ) {

				if (state < tsSuccess ) {
					/**
					 * 发生错误了
					 */
					oSlabFileManager->ResponseEcho(CacheAction::caClientReadResp, state, message, conn);
					return;
				}

				if(oSlabBlock.get() == NULL ) {
					/**
					 * 没有数据了
					 */
					oSlabFileManager->ResponseEcho(CacheAction::caClientReadResp, tsSuccess, rb_buffer.str(), conn);
					return;
				}

				int32_t lVersion = oSlabBlock->GetVersion();
				if ( lVersion <= 0) {
					/**
					 * 没有数据了
					 */
					oSlabFileManager->ResponseEcho(CacheAction::caClientReadResp, tsSuccess, rb_buffer.str(), conn);
					return;
				}

				std::string data;
				int bytes_readed = 0;

				if ( bytes_to_read > 0) {
					bytes_readed= oSlabBlock->Read(offset, block_offset_id, bytes_to_read, data);
				} else {
					/**
					 * 来自 Read2 函数，一次读取完整一块
					 */
					size_t used_size = oSlabBlock->GetUsedSize();
					bytes_readed= oSlabBlock->Read(offset, used_size, data);
				}

				if ( bytes_readed > 0 ) {
					rb_buffer.write(data.c_str(), bytes_readed);
				}

				if (offline == false ) {
					if ( ( lVersion == mVersion && bReport == true ) || lVersion > mVersion ) {
						/**
						 *
						 * 更新元数据版本到本地一致
						 *
						 **/
						LOGGER_TRACE(
								"#" << __LINE__ << ", SlabChainReader::ReadOneSlab, UpdateSlabMeta: " << filename << ", BlockId: "
								<< block_offset_id << ", Version: " << mVersion << " --> " << lVersion);

						oSlabFileManager->UpdateSlabMeta(block_offset_id, oSlabFile, oSlabBlock);

						std::shared_ptr<SlabMeta> oSlabMeta;
						if ( oSlabFile->GetMeta( block_offset_id, oSlabMeta ) == true && oSlabMeta.get() != NULL ) { //标记未不需要汇报这儿有数据，已经报过一次了
							oSlabMeta->bReport = false;
						}
					}
				}

				this->ReadAsync( offline );
				return;
			};

	/**
	 * 网络正常模式下，需要判断版本号，如果版本号小，从后端读取数据
	 */
	if (offline == false && mVersion < 1) {
		//说明元数据上面没有该数据，需要尝试直接从存储加载数据，本地内存就是有数据也不能用
		//两种情况下可能出现 1 数据被其他人删除了，2 重启元数据导致丢失，有可能 存储上面有 文件
		mVersion = 1;

		oSlabFile->RemoveBlock(block_offset_id);
		this->ReadBackend(block_offset_id, mVersion, callback, offline);
		return;
	}

	std::shared_ptr < SlabBlock > oSlabBlock = oSlabFile->GetBlock(block_offset_id);
	if (oSlabBlock.get() == NULL) {
		/**
		 * 离线模式下网络故障，不会从邻居读取数据
		 * 尝试从邻居读取数据，读取不成功，再从后端读取
		 */
		this->ReadOneSlabPeer(block_offset_id, mVersion, callback, offline);
		return;
	}
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
		callback(tsSuccess, "", oSlabBlock);
		return;
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainReader::ReadOneSlab, DropIt: " << filename << ", BlockId: "
					<< block_offset_id << ", lVersion: " << lVersion);

	/**
	 * 由于本地版本低于远程，需要 释放内存资源，并回收，重新获取数据后然后写入
	 *
	 */

	oSlabFile->RemoveBlock(block_offset_id);

	/**
	 * 如果 离线模式下网络故障 不会从邻居读取
	 * 尝试从邻居读取数据，读取不成功，再从后端读取
	 */
	this->ReadOneSlabPeer(block_offset_id, mVersion, callback, offline);
}
