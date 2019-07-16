INIT_IG(SlabChainWriter_watchdog, "SlabChainWriter");

SlabChainWriter::SlabChainWriter(const std::shared_ptr<SlabFileManager> & oSlabFileManager_,
		const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile> & oSlabFile_,
		const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_, const std::string & data_to_write,
		bool async_write_, const std::vector<uint32_t>& oBlockOffsetIds_) :
		SlabChainOp::SlabChainOp(oSlabFileManager_, serverdata_, oSlabFile_, conn_, offset_, oBlockOffsetIds_), async_write(
				async_write_), data_to_write(data_to_write) {
//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainWriter::SlabChainWriter" << ", " << (long) this);
	INC_IG (SlabChainWriter_watchdog);
}

SlabChainWriter::~SlabChainWriter() {
	DEC_IG (SlabChainWriter_watchdog); //
//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainWriter::~SlabChainWriter" << ", " << (long) this);
}

void SlabChainWriter::WriteAsync(bool offline) {
	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期
	io_strand->post([ this, self, offline ]() {
		this->Write( offline );
	});
}

/*
 * 写入数据过程:
 * 先检查本地内存是否有数据，如果本地内存有数据，检查本地内存数据版本和元数据节点的数据版本是否一致，如果一致，直接写入新数据，
 * 通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
 * 如果本地内存无数据，需要先读取数据到本地内存，然后写入新数据，通知元数据节点，数据更新成功，元数据节点通知其它节点删除副本。
 *
 * 通过异步调用依次处理每一块
 */
void SlabChainWriter::Write(bool offline) {
	if (oBlockOffsetIds.empty() == true) {
		/**
		 * 数据块全部读取完成
		 */
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabChainWriter::Write, Success: " << bytes_done << " bytes, offset: " << offset)

		/**
		 * 启动延迟 flush
		 */
		oSlabFile->ResetLazyFlush();

		if (bytes_done > 0) {
			oSlabFile->ClearAttr();

			if (offline == false) {
				/*
				 * 离线模式下网络故障
				 * 需要注意文件长度，元数据端存储最大文件长度
				 */
				oSlabFileManager->UpdateSlabAttr(oSlabFile, time(NULL), offset + bytes_done, false);
			}
		}

		oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, bytes_done, "", conn);
		return;
	}

	uint32_t block_offset_id = oBlockOffsetIds.front();
	oBlockOffsetIds.pop_front();

	WriteOneSlab(block_offset_id, offline);
}

void SlabChainWriter::WriteOneSlab(uint32_t block_offset_id, bool offline) {
	int32_t mVersion = 0;
	if (offline == false) {
		if (oSlabOffsetMetas.count(block_offset_id) > 0) {
			mVersion = oSlabOffsetMetas[block_offset_id].version;
		}
	}

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	if (iFisrtBlockOffsetId != block_offset_id && iLastBlockOffsetId != block_offset_id) {
		//不是第一或最后块，中间块是全覆盖，所以不需要从存储和其他节点获取数据，直接分配新空间，旧空间自动回收，直接写入数据就行
		SaveDataToNewSlab(block_offset_id, mVersion, offline);
		return;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabChainWriter::SaveDataToSlab: " << block_offset_id);
	/**
	 * 离线模式下网络故障，不需要判断版本号
	 */
	if (offline == false && mVersion < 1) {
		//说明元数据上面没有该数据，需要尝试直接从存储加载数据，本地内存就是有数据也不能用
		//两种情况下可能出现 1 数据被其他人删除了，2 重启元数据导致丢失
		//有可能 存储上面有 文件
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabChainWriter::SaveDataToSlab: Slab not exist, BlockId: " << block_offset_id);

		oSlabFile->RemoveBlock(block_offset_id);
		mVersion = 1;

		this->ReadBackend(block_offset_id, mVersion,
				[this, self, block_offset_id, mVersion, offline ] ( int state, const std::string & message,
						std::shared_ptr<SlabBlock> oSlabBlock ) {

					if(state == tsSuccess) {
						//从存储读取数据成功，已经在新建了内存块，保存数据到内存块和后端存储
						if (this->SaveDataToOldSlab(block_offset_id, mVersion, offline) == true) {
							return;
						}
					}

					SaveDataToNewSlab( block_offset_id, mVersion, offline );

				}, offline);
		return;
	}

	/**
	 * 尝试优先修改本地已有数据
	 */
	if (this->SaveDataToOldSlab(block_offset_id, mVersion, offline) == true) {
		return;
	}

	/**
	 * 写入数据之前，先尝试从邻居或底部存储加载数据过来，加载成功 state = tsSuccess
	 */
	this->ReadOneSlabPeer(block_offset_id, mVersion,
			[this, self, block_offset_id, mVersion, offline ] ( int state, const std::string & message, std::shared_ptr<SlabBlock> oSlabBlock ) {

				if(state == tsSuccess ) {
					//从存储读取数据成功，已经在新建了内存块，保存数据到内存块和后端存储
					if (this->SaveDataToOldSlab(block_offset_id, mVersion, offline) == true) {
						return;
					}
				}

				SaveDataToNewSlab( block_offset_id, mVersion, offline );
			}, offline);
}

void SlabChainWriter::SaveDataToNewSlab(uint32_t block_offset_id, int32_t mVersion, bool offline) {
	//直接新建内存块，保存数据
	std::shared_ptr < SlabBlock > oSlabBlock = oSlabFileManager->oSlabFactory->New();
	if (oSlabBlock.get() == NULL) {
		/**
		 * 如果系统故障，导致没有缓存块
		 */
		LOGGER_TRACE("#" << __LINE__ << ", SlabChainWriter::SaveDataToNewSlab, Failed: Out of Memory");
		oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, -ENOMEM, strerror(ENOMEM), conn);
		return;
	}

	oSlabBlock->SetVersion(mVersion < 1 ? 1 : mVersion);

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainWriter::SaveDataToNewSlab, Create new Slab: " << filename << ", BlockId: "
					<< block_offset_id << ", Version: " << oSlabBlock->GetVersion());

	int slab_length = oSlabBlock->Write(offset, block_offset_id, data_to_write); // 内部版本 ++
	oSlabFile->AddBlock(block_offset_id, oSlabBlock);
	/**
	 * 保存到后端存储
	 */
	int e_code = 0;
	std::string e_message;

	if (SaveBackend(block_offset_id, oSlabBlock, e_code, e_message) == true) {
		oSlabBlock->Commit();
		WriteSlabMeta(block_offset_id, oSlabBlock, slab_length, offline);
	} else {
		LOGGER_WARN(
				"#" << __LINE__ << ", SlabChainWriter::SaveDataToNewSlab: " << filename << ", Failed: " << e_message);
		oSlabBlock->Free();
		oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, -abs(e_code), e_message, conn);
	}
}

bool SlabChainWriter::SaveDataToOldSlab(uint32_t block_offset_id, int32_t mVersion, bool offline) {
	std::shared_ptr < SlabBlock > oSlabBlock = oSlabFile->GetBlock(block_offset_id);

	if (oSlabBlock.get() == NULL || oSlabBlock->IsValid() == false) {
		return false;
	}

	//如果本地内存有数据，检查本地内存数据版本与元数据版本是否一致，如果本地版本小于远程，删除本地内存数据，如果一致或大于，直接写入;

	int32_t lVersion = oSlabBlock->GetVersion();

	if (lVersion > 0 && lVersion >= mVersion) { //可以直接写入
		int slab_length = oSlabBlock->Write(offset, block_offset_id, data_to_write); // 内部版本 ++

		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabChainWriter::SaveDataToOldSlab: " << filename << ", BlockId: "
						<< block_offset_id << ", " << slab_length << " bytes, Version: " << oSlabBlock->GetVersion());
		/**
		 * 保存到后端存储
		 */
		int e_code = 0;
		std::string e_message;

		if (SaveBackend(block_offset_id, oSlabBlock, e_code, e_message) == true) {
			WriteSlabMeta(block_offset_id, oSlabBlock, slab_length, offline);
		} else {
			//后端写入失败，反馈给client
			LOGGER_WARN(
					"#" << __LINE__ << ", SlabChainWriter::SaveDataToOldSlab: " << filename << ", Failed: "
							<< e_message);
			oSlabBlock->Free();
			oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, -abs(e_code), e_message, conn);
		}

		return true;
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainWriter::SaveDataToOldSlab, DropIt: " << filename << ", BlockId: "
					<< block_offset_id);

	/**
	 * 由于本地版本低于远程，需要 释放内存资源，并回收，重新获取数据后然后写入
	 *
	 * 如果当前块完全覆盖，不需要重新获取，直接写入
	 *
	 */

	oSlabFile->RemoveBlock(block_offset_id);
	return false;
}

void SlabChainWriter::WriteSlabMeta(uint32_t block_offset_id, const std::shared_ptr<SlabBlock> & oSlabBlock,
		int slab_length, bool offline) {

	if (offline == true) {
		/**
		 * 写入一块成功，并且元数据信息确认了
		 * 继续处理下一块
		 */

		oSlabFile->AddBlock(block_offset_id, oSlabBlock);

		bytes_done += slab_length;
		this->WriteAsync(offline);
		return;
	}

	std::shared_ptr < TcpMessage > message = oSlabFileManager->NewMetaMessage(CacheAction::caSlabPutMeta);
	//获取所有块在元数据中的版本信息，存在一个风险，取回版本后，数据还保存完，元数据端被别人改了

	message->output->write_int32(oSlabFile->GetUuid());
	message->output->write_str(filename);

	message->output->write_uint32(block_offset_id);
	message->output->write_int32(oSlabBlock->GetVersion());

	message->output->write_uint16(oSlabFileManager->slab_port); /* 本地服务端口 */

	message->output->write_int8(1); //覆盖节点信息

	time_t mtime = oSlabFile->GetStatMtime();
	message->output->write_int64(mtime);

	off_t msize = oSlabFile->GetStatSize();
	message->output->write_int64(msize);

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainWriter::WriteOneSlabMetaInfo: " << filename << ", BlockId: "
					<< block_offset_id << ", Version: " << oSlabBlock->GetVersion());

	auto self = this->shared_from_this();    //异步函数，通过自引用传递保持生命周期

	message->callback =
			[ this, self, block_offset_id, oSlabBlock, slab_length, offline ]( std::shared_ptr<stringbuffer> input,
					const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {

				//网络故障处理，当发送不接受任务后，服务端会关闭连接
				if(ec) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabChainWriter::WriteOneSlabMetaInfo: " << filename << ", Error: " << ec.message() );
					oSlabBlock->Free();
					oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, - EIO, ec.message() , conn);
					return;
				}

				int8_t action;
				if ( input->read_int8( action ) == false || action != CacheAction::caSlabPutMetaResp ) {
					LOGGER_WARN( "#" << __LINE__ << ", SlabChainWriter::WriteOneSlabMetaInfo: "<< filename << ", Error: Invalid response");
					oSlabBlock->Free();
					oSlabFileManager->ResponseEcho(CacheAction::caClientWriteResp, - EIO, strerror(EIO) , conn);
					return;
				}

				/**
				 * 写入一块成功，并且元数据信息确认了
				 * 继续处理下一块
				 */

				oSlabFile->AddBlock( block_offset_id, oSlabBlock );

				bytes_done += slab_length;
				this->WriteAsync( offline );
			};

	oSlabFileManager->PostMessage(message);
}

/**
 * 根据参数支持同步写入和异步写入底层存储功能
 * 采用延时写入方式，专门构建写入队列，来写入数据
 */
bool SlabChainWriter::SaveBackend(size_t block_offset_id, const std::shared_ptr<SlabBlock> & oSlabBlock, int & code,
		std::string & message) {

	if (oBackendManager->IsMemory(filename) == true) {
		return true;
	}

	if (async_write == true) {
		/**
		 * 标志为读写缓存块，内部暂时不会被 gc 了，但是数据还是会被其他读取，只有 flush 或 close 将数据放入后端
		 * flush 和 write 必须按块加锁，来避免读写冲突
		 */

		oSlabBlock->SetEditable(true);
		oSlabFile->AddDirty(block_offset_id, oSlabBlock);

		code = 0;
		return true;
	}

	size_t used_size = oSlabBlock->GetUsedSize();
	if (used_size == 0) {
		return true;
	}

	/**
	 * 如果是后端存储数据，异步直接写入
	 */
	std::shared_ptr < Backend > oBackend = oBackendManager->Open(filename, true);
	if (oBackend.get() == NULL) {
		code = EINVAL;
		message = FAILED_INVALID_ARGUMENT;
		LOGGER_TRACE("#" << __LINE__ << ", SlabChainWriter::SaveBackend, Error: " << message << ", " << filename);
		return false;
	}

	std::string data;
	int bytes_readed = oSlabBlock->Read(0, used_size, data);
	if (bytes_readed == 0) {
		return true;
	}

	if (bytes_readed < 0) { //数据块被 gc 回收了
		code = ENOMEM;
		message = strerror(ENOMEM);

		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabChainWriter::SaveBackend, Error: " << message << ", " << filename
						<< ", BlockId: " << block_offset_id);
		return false;
	}

	uint64_t offset = block_offset_id * SIZEOFBLOCK;

	int bytes = oBackend->Write((void *) data.c_str(), bytes_readed, offset);
	if (bytes > 0) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabChainWriter::SaveBackend: " << bytes << " bytes for " << filename
						<< ", BlockId: " << block_offset_id);
		return true;
	}

	code = oBackend->code;
	message = oBackend->message;

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabChainWriter::SaveBackend, Error: " << message << ", " << filename << ", BlockId: "
					<< block_offset_id);

	return false;
}

