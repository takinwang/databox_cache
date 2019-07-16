/*
 * SlabFile.cpp
 *
 *  Created on: Nov 23, 2018
 *      Author: root
 */

#include <databox/cpl_memdog.hpp>
INIT_IG(SlabFile_watchdog, "SlabFile");

SlabFile::SlabFile(SlabFileManager * pManager_, const std::shared_ptr<SlabFactory>& oSlabFactory_,
		const std::string & filename_, bool memory_) :
		oSlabBlocks(BLOCK_CACHE_MAX), oSlabMetas(META_CACHE_MAX), pManager(pManager_), oSlabFactory(oSlabFactory_), //
		filename(filename_), bMemoryFile(memory_) {

	LOGGER_TRACE("#" << __LINE__ << ", SlabFile::SlabFile, " << filename << ", oid: " << (long) this);
	INC_IG(SlabFile_watchdog);

	uuid = time(NULL);
	oCallBarrier = std::make_shared<mtsafe::CallBarrier<bool>>();

	io_strand = AsyncIOService::getInstance()->getStrand();
	io_service = AsyncIOService::getInstance()->getIoService();
}

/**
 * 存在一个问题就是当 client 异步写入数据后，没有调用 flush，由于大批量读写新文件，导致在 定时器启动 flush 之前
 * 由于slabfile数量太多，超过 lrucache 容量，导致 当前 slabfile 被自动 清除了，写入的数据可能未被持久化
 */
SlabFile::~SlabFile() {
	LOGGER_TRACE("#" << __LINE__ << ", SlabFile::~SlabFile, " << filename << ", " << (long) this);
	DEC_IG(SlabFile_watchdog);

	if (timer_Flush.get()) {
		boost::system::error_code ec;
		timer_Flush->cancel();
		timer_Flush.reset();
	}

	ClearBlocks();
}

void SlabFile::ResetLazyFlush() {
	if (this->IsMemoryFile() == true) {
		return;
	}

	std::lock_guard < std::mutex > lock(mtx_flush);
	if (timer_Flush.get()) {
		boost::system::error_code ec;
		timer_Flush->cancel();
		timer_Flush.reset();
	}

	timer_Flush = std::make_shared < boost::asio::deadline_timer > (*io_service);
	auto self = this->shared_from_this();

	/**
	 * 由于处于定时刷新过程中， self 已经自引用，所以不会出现退出，只有完成 FlushDirty 任务之后，才会关闭
	 */

	timer_Flush->expires_from_now(boost::posix_time::seconds(TIMER_FLUSH_TTL));
	timer_Flush->async_wait([ this, self ](const boost::system::error_code &ec) {

		if(ec) {
			return;
		}

		LOGGER_TRACE("#" << __LINE__ << ", SlabFile::ResetFlush, Start FlushDirty: " << filename);

		this->FlushDirty( nullptr );
	});
}

void SlabFile::CancelLazyFlush() {

	std::lock_guard < std::mutex > lock(mtx_flush);
	if (timer_Flush.get()) {
		boost::system::error_code ec;
		timer_Flush->cancel();
		timer_Flush.reset();
	}

}

std::shared_ptr<SlabBlock> SlabFile::GetBlock(size_t block_offset_id) {
	std::weak_ptr < SlabBlock > oOldSlabBlock;
	if (oSlabBlocks.get(block_offset_id, oOldSlabBlock) == 1) {

		std::shared_ptr < SlabBlock > oNewSlabBlock = oOldSlabBlock.lock();
		if (oNewSlabBlock.get() == NULL) {
			return oNewSlabBlock;
		}

		if (oNewSlabBlock->GetVersion() > 0) {
			return oNewSlabBlock;
		}

		//可能被缓存回收程序回收了
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabFile::GetBlock, Erase empty Block: " << this->filename << ", BlockId: "
						<< block_offset_id);

		oSlabBlocks.erase(block_offset_id);
	}

	return std::shared_ptr<SlabBlock>();
}

void SlabFile::AddBlock(size_t block_offset_id, const std::shared_ptr<SlabBlock>& oNewSlabBlock) {
	std::weak_ptr < SlabBlock > oOldSlabBlock;
	if (oSlabBlocks.pop(block_offset_id, oOldSlabBlock) == 1) {

		std::shared_ptr < SlabBlock > oSlabBlock = oOldSlabBlock.lock();
		if (oSlabBlock.get() != NULL && oSlabBlock.get() != oNewSlabBlock.get()) {
			oSlabBlock->Free();
		}
	}

	oSlabBlocks.put(block_offset_id, std::weak_ptr < SlabBlock > (oNewSlabBlock), 0);
}

void SlabFile::RemoveBlock(size_t block_offset_id) {
	std::weak_ptr < SlabBlock > oOldSlabBlock;
	if (oSlabBlocks.pop(block_offset_id, oOldSlabBlock) == 1) {

		std::shared_ptr < SlabBlock > oNewSlabBlock = oOldSlabBlock.lock();
		if (oNewSlabBlock.get() != NULL) {
			oNewSlabBlock->Free();
		}

	}
}

void SlabFile::ClearBlocks() {
	oSlabBlocks.clear([ this ]( size_t block_id, const std::weak_ptr<SlabBlock> & oSlabBlock ) {
		std::shared_ptr < SlabBlock > oNewSlabBlock = oSlabBlock.lock();

		if (oNewSlabBlock.get() != NULL) {
			oNewSlabBlock->Free();
		}

	});
}

std::shared_ptr<mtsafe::CallBarrier<bool>> SlabFile::GetBarrier(size_t block_offset_id) {
	std::shared_ptr < mtsafe::CallBarrier<bool> > oCallBarrier;
	oCallBarriers.get_or_create(block_offset_id, oCallBarrier, [this]() {
		return std::make_shared< mtsafe::CallBarrier<bool> >( );
	});
	return oCallBarrier;
}

std::shared_ptr<mtsafe::CallBarrier<bool>> SlabFile::GetBarrier() {
	return oCallBarrier;
}

bool SlabFile::GetMeta(size_t block_offset_id, std::shared_ptr<SlabMeta>& oSlabMeta) {
	return oSlabMetas.get(block_offset_id, oSlabMeta);
}

void SlabFile::PutMeta(size_t block_offset_id, const SlabMeta& oSlabMeta, uint32_t ttl) {
	std::shared_ptr < SlabMeta > tmpSlabMeta = std::make_shared<SlabMeta>();
	tmpSlabMeta->CopyFrom(&oSlabMeta);
	oSlabMetas.put(block_offset_id, tmpSlabMeta, ttl);
}

void SlabFile::RemoveMeta(size_t block_offset_id) {
	oSlabMetas.erase(block_offset_id);
}

void SlabFile::ClearMeta() {
	oSlabMetas.clear();
}

void SlabFile::ClearAttr() {
	stat_mtime = 0;
	stat_size = 0;
}

/**
 * 异步写入时候，标记块为 弄需要写入后端
 */
void SlabFile::AddDirty(size_t block_offset_id, const std::shared_ptr<SlabBlock>& oSlabBlock) {
	/*
	 * 如果不存在，加入
	 */
	oDirtyBlocks.put(block_offset_id, std::weak_ptr < SlabBlock > (oSlabBlock));
}

void SlabFile::ClearDirty() {
	auto self = this->shared_from_this();
	oDirtyBlocks.clear([ this, self ]( size_t block_offset_id, const std::weak_ptr < SlabBlock > & oSlabBlock ) {

		std::shared_ptr < SlabBlock > oNewSlabBlock = oSlabBlock.lock();

		if (oNewSlabBlock.get() != NULL) {
			oNewSlabBlock->SetEditable(false);
		}

	});
}

void SlabFile::FlushDirtyOne(const std::function<void(ssize_t state, const std::string & message)> & callback) {
	/**
	 * 刷数据的时候后阻止针对该文件的 getattr，flush，close 和 定时器
	 */
	auto self = this->shared_from_this();
	const auto & flushit = [ this, self, callback ]() {
		/**
		 * 内部顺序执行往后台刷数据
		 */

		size_t block_offset_id;
		std::weak_ptr < SlabBlock > oSlabBlock;

		if (oDirtyBlocks.pop_front(block_offset_id, oSlabBlock) == 0) {
			/**
			 * 之前任务已经处理完成了，后续没有数据块需要写入后端
			 */
			ClearAttr();

			if (callback) {
				callback( tsSuccess, "");
			}
			return true;
		}

		std::shared_ptr < SlabBlock > oNewSlabBlock = oSlabBlock.lock();

		if (oNewSlabBlock.get() == NULL) { //已经被 gc 掉了，理论上不应该出现
				io_strand->post([ this, self, callback ]() {
							this->FlushDirtyOne( callback );
						});

				return true;
			}

			if (oNewSlabBlock->GetVersion() < 1) {
				io_strand->post([ this, self, callback ]() {
							this->FlushDirtyOne( callback );
						});

				return true;
			}

			size_t used_size = oNewSlabBlock->GetUsedSize();
			if (used_size == 0) {

				oNewSlabBlock->SetEditable(false);

				io_strand->post([ this, self, callback ]() {
							this->FlushDirtyOne( callback );
						});

				return true;
			}

			/**
			 * 如果是后端存储数据，直接写入
			 */
			std::shared_ptr < Backend > oBackend = pManager->oBackendManager->Open(filename, true);
			if (oBackend.get() == NULL) {
				oNewSlabBlock->SetEditable(false);

				LOGGER_TRACE(
						"#" << __LINE__ << ", SlabFile::FlushDirtyOne, Error: " << FAILED_INVALID_ARGUMENT << ", " << filename
						<< ", BlockId: " << block_offset_id);
				/**
				 * 发生故障，缓存数据作废，没法写入了
				 */
				ClearDirty();

				if (callback) {
					callback( -EINVAL, FAILED_INVALID_ARGUMENT);
				}
				return true;
			}

			/**
			 * 需要采用值拷贝
			 */
			std::string data;
			int bytes_readed = oNewSlabBlock->Read(0, used_size, data);
			oNewSlabBlock->SetEditable(false);

			if (bytes_readed == 0) {
				io_strand->post([ this, self, callback ]() {
							this->FlushDirtyOne( callback );
						});
				return true;
			}

			if (bytes_readed < 0) {
				/**
				 * 数据块被 gc 回收了
				 * 理论上不会发生
				 */

				LOGGER_TRACE(
						"#" << __LINE__ << ", SlabFile::FlushDirtyOne, Error: " << strerror(ENOMEM) << ", " << filename
						<< ", BlockId: " << block_offset_id);

				ClearDirty();

				if (callback) {
					callback( -ENOMEM, strerror(ENOMEM));
				}
				return true;
			}

			uint64_t offset = block_offset_id * SIZEOFBLOCK;

			int bytes = oBackend->Write((void *) data.c_str(), bytes_readed, offset);
			if (bytes > 0) {
				LOGGER_TRACE(
						"#" << __LINE__ << ", SlabFile::FlushDirtyOne: " << bytes << " bytes for " << filename << ", BlockId: "
						<< block_offset_id);

				io_strand->post([ this, self, callback ]() {
							this->FlushDirtyOne( callback );
						});
				return true;
			}

			LOGGER_TRACE(
					"#" << __LINE__ << ", SlabFile::FlushDirtyOne, Error: " << oBackend->message << ", " << filename
					<< ", BlockId: " << block_offset_id);
			/**
			 * 发生故障，缓存数据作废，没法写入了
			 */
			ClearDirty();

			if (callback) {
				callback( -abs(oBackend->code), oBackend->message);
			}
			return true;
		};

	oCallBarrier->CallSync(flushit);
	return;
}

/**
 * 刷新修改数据到后端
 * flush，close 和 定时器 调用
 */
void SlabFile::FlushDirty(const std::function<void(ssize_t state, const std::string & message)> & callback) {

	if (this->IsMemoryFile() == true) {
		ClearDirty();
		if (callback) {
			callback(tsSuccess, "");
		}
		return;
	}

	auto self = this->shared_from_this();

	io_strand->post([ this, self, callback ]() {
		if (oDirtyBlocks.size() == 0) {
			if (callback) {
				callback( tsSuccess, "");
			}
			return;
		}

		LOGGER_TRACE("#" << __LINE__ << ", SlabFile::FlushDirty, " << filename << ", Blocks: " << oDirtyBlocks.size());
		this->FlushDirtyOne( callback );
	});
}

size_t SlabFile::TraceTrushes() {
	int nItems = oSlabBlocks.size();
	if (nItems == 0) {
		return 0;
	}

	auto self = this->shared_from_this();
	for (int i = 0; i < nItems; i++) {
		size_t block_id;
		std::weak_ptr < SlabBlock > oSlabBlock;

		oSlabBlocks.gc(block_id, oSlabBlock, [this, self](const std::weak_ptr< SlabBlock > & gSlabBlock ) {
			if( gSlabBlock.expired() == true ) {
				return false;
			}
			return true;
		});
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabFile::TraceTrushes, " << filename << ", Valid: " << oSlabBlocks.size()
					<< " blocks, Expired: " << nItems - oSlabBlocks.size() << " blocks");

	return oSlabBlocks.size();
}
