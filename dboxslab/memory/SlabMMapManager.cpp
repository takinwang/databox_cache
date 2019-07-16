#include "SlabMMapManager.hpp"

#include <databox/cpl_memdog.hpp>
INIT_IG(SlabMMapBlock_watchdog, "SlabMMapBlock");

/**
 * 没有进行 0 填充，由于写入有些是写入到内存缓存上，内存缓存不够的时候，将整个内存块 copy 到 swap 缓存，不会出现垃圾数据
 */
SlabMMapBlock::SlabMMapBlock(SlabMMapManager * pManager_, SlabMemFactory *pMemFactory_, int fd_, size_t slab_id_,
		size_t block_id_) :
		SlabBlock::SlabBlock(slab_id_, block_id_), fd(fd_), pMemFactory(pMemFactory_), pManager(pManager_) {
//	LOGGER_TRACE(
//			"#" << __LINE__ << ", SlabMMapBlock::SlabMMapBlock: " << fd<< ", " << slab_id << "-" << block_id << ", " << ( long ) this);

	INC_IG(SlabMMapBlock_watchdog);
}

SlabMMapBlock::~SlabMMapBlock() {
	DEC_IG(SlabMMapBlock_watchdog);

	ResetSlabMem();

//	LOGGER_TRACE(
//			"#" << __LINE__ << ", SlabMMapBlock::~SlabMMapBlock: " << fd<< ", " << slab_id << "-" << block_id << ", " << ( long ) this);
}

/**
 * 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
 *  同时将块缓存放入到正在使用队列中
 * 多线程安全
 */
void SlabMMapBlock::Commit() {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) {        //可能 Free 已经调用，将自己 gc 或 free 了
		return;
	}

	auto self = this->shared_from_this();
	pManager->Commit(self);
}

/**
 * 释放当前 块缓存 到 资源池，不管是 只读或读写模式，都被 Free，clone 一个新对象重新放入到队列
 * 多线程安全
 */
std::shared_ptr<SlabBlock> SlabMMapBlock::Free() {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) {        //可能 Free 已经调用，将自己 gc 或 free 了
		return std::shared_ptr<SlabBlock>();
	}

	ResetSlabMem();

	std::shared_ptr<SlabMMapBlock> oSlabMMapBlock = std::make_shared<SlabMMapBlock>(pManager, pMemFactory, fd, slab_id,
			block_id);
	std::shared_ptr<SlabBlock> oNewSlabBlock = std::static_pointer_cast<SlabBlock, SlabMMapBlock>(oSlabMMapBlock);

	pManager->Free(oNewSlabBlock);

	version = 0;
	pManager = NULL;

	return oNewSlabBlock;
}

void SlabMMapBlock::SetEditable(bool t) {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) {        //可能 Free 已经调用，将自己 gc 或 free 了
		return;
	}

	this->bEditable = t;

	auto self = this->shared_from_this();
	pManager->SwitchEditable(self);
}

/**
 * 被加锁函数内调用，所以多线程安全
 */
int SlabMMapBlock::ReadToBuffer(char* buffer, size_t size) {
	if (fd < 0) {
		return -1;
	}

	if (size == 0) {
		return 0;
	}

	if (pManager && pManager->Terminated() == true) {
		return -1;
	}

	int page_size = getpagesize();
	off_t offset = block_id * SIZEOFBLOCK;
	off_t pa_offset = offset & ~(page_size - 1);

	size_t length = std::min<size_t>(size, SIZEOFBLOCK);
	size_t mmap_length = length + offset - pa_offset;

	char * addr = (char *) mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, pa_offset);
	if (addr == MAP_FAILED) {
		LOGGER_ERROR("SlabMMapBlock::Read, mmap error: " << strerror(errno));
		return -1;
	}

	char * ptr_src = addr + offset - pa_offset;
	memcpy(buffer, ptr_src, length);

	if (munmap(addr, mmap_length) < 0) {
		LOGGER_ERROR("SlabMMapBlock::Read, munmap error: " << strerror(errno));
		return -1;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabMMapBlock::Read, offset: " << offset << ", size: " << length);
	return length;
}

/**
 * 被加锁函数内调用，所以多线程安全
 */
int SlabMMapBlock::WriteFromBuffer(const char* buffer, size_t size) {
	if (fd < 0) {
		return -1;
	}
	if (size == 0) {
		return 0;
	}

	if (pManager && pManager->Terminated() == true) {
		return -1;
	}

	int page_size = getpagesize();
	off_t offset = block_id * SIZEOFBLOCK;
	off_t pa_offset = offset & ~(page_size - 1);

	size_t length = std::min<size_t>(size, SIZEOFBLOCK);
	size_t mmap_length = length + offset - pa_offset;

	char * addr = (char *) mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, pa_offset);
	if (addr == MAP_FAILED) {
		LOGGER_ERROR("#" << __LINE__ << ", SlabMMapBlock::Write, mmap error: " << strerror(errno));
		return -1;
	}

	char * ptr_dst = addr + offset - pa_offset;
	memcpy(ptr_dst, buffer, length);

	if (munmap(addr, mmap_length) < 0) {
		LOGGER_ERROR("#" << __LINE__ << ", SlabMMapBlock::Write, munmap error: " << strerror(errno));
		return -1;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabMMapBlock::Write, offset: " << offset << ", size: " << length);
	return length;
}

/**
 * 在读取和写入之前将数据从本地磁盘copy到内存缓存，同步 used_size 和 version
 * 被加锁函数内调用，所以多线程安全
 */
std::shared_ptr<SlabBlock> SlabMMapBlock::CheckForIO() {

	/**
	 * 废内存块，被 gc 或 free 了，重置
	 */
	if (oSlabMemBlock.get() != NULL && oSlabMemBlock->IsValid() == false) {
		oSlabMemBlock.reset();
	}

	std::shared_ptr<SlabBlock> oNewSlabBlock;
	/**
	 * 如果关联了内存块，直接使用
	 */
	if (oSlabMemBlock.get() != NULL) {
		return oNewSlabBlock;
	}

	/**
	 * 获取新内存空间，可能存在NULL？调用者函数已经保障安全了
	 */
	oNewSlabBlock = pMemFactory->New();
	if (oNewSlabBlock.get() == NULL) {
		return oNewSlabBlock;
	}

	oSlabMemBlock = std::static_pointer_cast<SlabMemBlock, SlabBlock>(oNewSlabBlock);

	/**
	 * 当 该内存块被 gc 的时候进行回调，此处不需要 自引用，自己被 gc 或 free 的时候，已经 free 了对应的 oSlabMemBlock 内存块
	 * callback 已经被清空，不会发送回调
	 */
	oSlabMemBlock->SetCallback(
			[ this ](const std::shared_ptr<SlabMemBlock> & oSlabBlock ) {
				/**
				 * 从内存块复制数据到磁盘块，回调前已经加锁保障线程安全
				 */

				used_size = oSlabBlock->used_size;
				version = oSlabBlock->version.load();

				if (used_size > 0) {
					/**
					 * 内存有数据，将内存数据copy到本地swap
					 */
					LOGGER_TRACE(
							"#" << __LINE__ << ", SlabMMapBlock::WriteFromBuffer: " << used_size << " bytes from SlabMemBlock: " << oSlabBlock->GetSlabId() //
							<< "-" << oSlabBlock->GetBlockId( ) << ", to Swap: " << this->GetSlabId( ) << "-" << this->GetBlockId());

					this->WriteFromBuffer(oSlabBlock->pBuffer, used_size);
				}

				oSlabMemBlock.reset();
				/* 在被 gc 情况下调用，在 oSlabMemBlock 生命周期内有效 */
				return;
			});

	/**
	 * 将数据复制到内存
	 */
	if (used_size > 0) {

		if (this->ReadToBuffer(oSlabMemBlock->pBuffer, used_size) < 0) {
			oSlabMemBlock.reset();

			oNewSlabBlock->Free();
			oNewSlabBlock.reset();
			return oNewSlabBlock;
		}

		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMMapBlock::ReadToBuffer: " << used_size << " bytes from Swap: " << this->GetSlabId( ) << "-" << this->GetBlockId() //
				<< ", to SlabMemBlock: " << oNewSlabBlock->GetSlabId() << "-" << oNewSlabBlock->GetBlockId( ));
	}

	oSlabMemBlock->used_size = used_size;
	oSlabMemBlock->version = version.load();

	return oNewSlabBlock;
}

/**
 * 被加锁函数内调用，所以多线程安全
 */
void SlabMMapBlock::ResetSlabMem() {
	if (oSlabMemBlock.get() == NULL) {
		return;
	}

	oSlabMemBlock->Free();
	oSlabMemBlock.reset();
}

/**
 * 初次加载数据的时候，在内存块上写入数据，version 不变，dirty 不变，不是修改数据！！！
 * ptr 待写入的数据位置，数据将从头读取
 * size 待写入的数据大小
 * offset 待写入缓存区的位置偏移，小于 0，则为 append 模式，从数据缓冲区最后开始写入
 * 返回成功写入数据量，< 0 表示 offset > buffer_size
 *
 * 存在多线程调用，加锁
 */
int SlabMMapBlock::WriteBlock(const char* ptr_src, size_t size, int offset) {
	if (ptr_src == NULL || size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	std::shared_ptr<SlabBlock> oNewSlabBlock = CheckForIO();
	if (oSlabMemBlock.get() == NULL) {
		return -1;
	}

	int bytes = oSlabMemBlock->WriteBlock(ptr_src, size, offset);

	if (bytes < 0) {
		ResetSlabMem();
		return bytes;
	}

	used_size = oSlabMemBlock->GetUsedSize();
	version = oSlabMemBlock->GetVersion();

	if (oNewSlabBlock.get() != NULL) {
		oNewSlabBlock->Commit();
	}

	return bytes;
}

/**
 * 存在多线程调用，加锁
 */
int SlabMMapBlock::Write(uint64_t offset, uint32_t block_offset_id, const std::string& data) {
	if (data.size() <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	std::shared_ptr<SlabBlock> oNewSlabBlock = CheckForIO();
	if (oSlabMemBlock.get() == NULL) {
		return -1;
	}

	int bytes = oSlabMemBlock->Write(offset, block_offset_id, data);

	if (bytes < 0) {
		ResetSlabMem();
		return bytes;
	}

	used_size = oSlabMemBlock->GetUsedSize();
	version = oSlabMemBlock->GetVersion();
//	bDirty = true;

	if (oNewSlabBlock.get() != NULL) {
		oNewSlabBlock->Commit();
	}

	return bytes;
}

/**
 * 存在多线程调用，加锁
 *
 */
int SlabMMapBlock::Read(int offset, size_t size, std::string & ptr_dst) {
	if (size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //   可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	std::shared_ptr<SlabBlock> oNewSlabBlock = CheckForIO();
	if (oSlabMemBlock.get() == NULL) {
		return -1;
	}

	int bytes_readed = oSlabMemBlock->Read(offset, size, ptr_dst);

	if (bytes_readed < 0) {
		ResetSlabMem();
		return bytes_readed;
	}

	if (oNewSlabBlock.get() != NULL) {
		oNewSlabBlock->Commit();
	}

	return bytes_readed;
}

/**
 * 存在多线程调用，加锁
 */
int SlabMMapBlock::Read(uint64_t offset, uint32_t block_offset_id, size_t size, std::string & ptr_dst) {
	if (size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	std::shared_ptr<SlabBlock> oNewSlabBlock = CheckForIO();
	if (oSlabMemBlock.get() == NULL) {
		return -1;
	}

	int bytes = oSlabMemBlock->Read(offset, block_offset_id, size, ptr_dst);

	if (bytes < 0) {
		ResetSlabMem();
		return bytes;
	}

	if (oNewSlabBlock.get() != NULL) {
		oNewSlabBlock->Commit();
	}

	return bytes;
}

void SlabMMapBlock::SetVersion(const int32_t version) {
	SlabBlock::SetVersion(version);

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return;
	}

	if (oSlabMemBlock.get() != NULL && oSlabMemBlock->IsValid() == false) {
		ResetSlabMem();
	}

	if (oSlabMemBlock.get() != NULL) {
		oSlabMemBlock->version = version;
	}
}

SlabMMapManager::SlabMMapManager(const std::shared_ptr<SlabMemFactory>& oSlabMemFactory_, const std::string & root,
		size_t SlabId_) :
		oSlabMemFactory(oSlabMemFactory_), slab_id(SlabId_), read_blocks_map(NUMBLOCKS * 2), write_blocks_map(
		NUMBLOCKS * 2) {

	size_t path_id = (slab_id / 256) % 256;

	std::stringstream ss;
	ss << root << PathSep << std::uppercase << std::hex << std::setw(2) << std::setfill('0') << path_id;
	std::string path = ss.str();

	if (FileSystemUtils::MakeDirs(path) == false) {
		std::stringstream ss;
		ss << strerror(errno) << ": " << filename;
		throw dbox_error(ss.str());
	}

	ss << PathSep << std::uppercase << std::hex << std::setw(8) << std::setfill('0') << slab_id << ".swap";
	filename = ss.str();

	mode_t mode = S_IRUSR | S_IWUSR; //| S_IRGRP | S_IROTH;
	fd = open(filename.c_str(), O_RDWR | O_CREAT, mode);

	if (fd < 0) {
		LOGGER_ERROR(
				"#" << __LINE__ << ", SlabMMapManager::SlabMMapManager, " << filename << ", Error: " << strerror(errno));
		std::stringstream ss;
		ss << strerror(errno) << ": " << filename;
		throw dbox_error(ss.str());
		return;
	}

	size_t filesize = NUMBLOCKS * SIZEOFBLOCK;
	if (ftruncate(fd, filesize) < 0) {
		LOGGER_ERROR(
				"#" << __LINE__ << ", SlabMMapManager::SlabMMapManager, " << filename << ", Error: " << strerror(errno));
		Close();
		std::stringstream ss;
		ss << strerror(errno) << ": " << filename;
		throw dbox_error(ss.str());

		return;
	}

//	LOGGER_TRACE("#" << __LINE__ << ", SlabMMapManager::SlabMMapManager, " << filename << ", Size: " << filesize);

	for (size_t block_id = 0; block_id < NUMBLOCKS; block_id++) {
		idle_blocks_queue.push_back(
				std::shared_ptr<SlabBlock>(new SlabMMapBlock(this, oSlabMemFactory.get(), fd, slab_id, block_id)));
	}
}

SlabMMapManager::~SlabMMapManager() {
	Close();
}

void SlabMMapManager::Close() {
	read_blocks_map.clear();
	write_blocks_map.clear();
	idle_blocks_queue.clear();

	if (fd >= 0) {
		close(fd);
	}
	fd = -1;

	LOGGER_TRACE("#" << __LINE__ << ", SlabMMapManager::Close, SlabId: " << this->slab_id << ", File: " << filename);
	unlink(filename.c_str());
}

/**
 * 确认资源被使用了
 * 多线程安全
 */
void SlabMMapManager::Commit(const std::shared_ptr<SlabBlock>& oSlabBlock) {
	std::lock_guard<std::mutex> lock(mtx);

	size_t block_id = oSlabBlock->GetBlockId();
	if (read_blocks_map.exists(block_id) == true || write_blocks_map.exists(block_id) == true) {
		return;
	}

	//为了保障 read_blocks_map 和 write_blocks_map 一致性，需要加锁

	if (oSlabBlock->IsEditable() == true) {
		read_blocks_map.erase(block_id);
		write_blocks_map.put(block_id, oSlabBlock, 0);
	} else {
		write_blocks_map.erase(block_id);
		read_blocks_map.put(block_id, oSlabBlock, 0);
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabMMapManager::Commit, SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() << ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
}

/**
 * 释放当前 块缓存 到 资源池
 * 多线程调用安全
 */
void SlabMMapManager::Free(const std::shared_ptr<SlabBlock>& oNewSlabBlock) {
	size_t block_id = oNewSlabBlock->GetBlockId();

	{
		std::lock_guard<std::mutex> lock(mtx);
//		为了保障 read_blocks_map 和 write_blocks_map 一致性，需要加锁
		read_blocks_map.erase(block_id);
		write_blocks_map.erase(block_id);
	}

	idle_blocks_queue.push_back(oNewSlabBlock);
	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabMMapManager::Free, SlabId: " << oNewSlabBlock->GetSlabId() << ", BlockId: " << oNewSlabBlock->GetBlockId() << ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
}

std::shared_ptr<SlabBlock> SlabMMapManager::SwitchEditable(const std::shared_ptr<SlabBlock>& oSlabBlock) {
	std::lock_guard<std::mutex> lock(mtx);

	bool t = oSlabBlock->IsEditable();
	//		为了保障 read_blocks_map 和 write_blocks_map 一致性，需要加锁

	size_t block_id = oSlabBlock->GetBlockId();
	if (t == true) {
		read_blocks_map.erase(block_id);
		write_blocks_map.put(block_id, oSlabBlock, 0);
	} else {
		write_blocks_map.erase(block_id);
		read_blocks_map.put(block_id, oSlabBlock, 0);
	}
	//
	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabMemManager::SwitchEditable, SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() //
			<< ", Queue: " << idle_blocks_queue.qsize() << ", Used for Read: " << read_blocks_map.size() << ", Used for Write: " << write_blocks_map.size());

	return oSlabBlock;
}

/**
 * 从 资源池 分配一个 块缓存，状态变为 正在使用
 *由 New 获取的块对象，必须调用 Commit 或 Free
 * 多线程安全，由于上级是轮询方式，所以性能问题不大
 * 正常情况下，GC 会释放空白块，特殊情况下还是会没有块
 *
 */
std::shared_ptr<SlabBlock> SlabMMapManager::New() {
	std::shared_ptr<SlabBlock> oSlabBlock;
	if (fd < 0) {
		return oSlabBlock;
	}

	if (idle_blocks_queue.pop_front(oSlabBlock) == true && oSlabBlock.get() != NULL) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMMapManager::New, Idle SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() << ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
		return oSlabBlock;
	}

	/**
	 * 正常情况下，GC 会释放空白块，特殊情况下还是会没有块，需要调用者维护
	 */
	size_t block_id;
	if (read_blocks_map.gc(block_id, oSlabBlock, []( const std::shared_ptr< SlabBlock > & ) {
		return true;
	}) < 0 || oSlabBlock.get() == NULL) {
		//可能出现取不到的问题，需要由调用 New 函数的程序进行处理
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMMapManager::New, Empty Slab, Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
		return oSlabBlock;
	}

	//GC 处理

	/** 由于原有对象被其他引用，需要 Clone 一个新对象重新放入到队列，原有对象 version = 0，buffer = NULL 状态变为不可用
	 * Clone 已经加锁保障，重复调用，后续返回 空 对象
	 */

	oSlabBlock->Free();
	oSlabBlock.reset();

	if (idle_blocks_queue.pop_front(oSlabBlock) == true && oSlabBlock.get() != NULL) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMMapManager::New, Idle SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() << ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
		return oSlabBlock;
	}

	return oSlabBlock;
}

SlabMMapFactory::SlabMMapFactory(const std::shared_ptr<SlabMemFactory> & oSlabMemFactory_, const std::string & root,
		size_t maxSlabs_) :
		oSlabMemFactory(oSlabMemFactory_) {

	memory_size = StringUtils::FormatBytes(static_cast<uint64_t>(maxSlabs_) * SIZEOFBLOCK * NUMBLOCKS);

	LOGGER_INFO(
			"#" << __LINE__ << ", SlabMMapManager::SlabMMapManager, Start allocate swap: " << memory_size << ", Root: " << root);

	for (size_t slab_id = 0; slab_id < maxSlabs_; slab_id++) {
		std::shared_ptr<SlabMMapManager> oSlab(new SlabMMapManager(oSlabMemFactory_, root, slab_id));

		if (oSlab->IsValid() == false) { //没有空间了
			oSlab->Close();
			break;
		}

		oSlabManagers.push_back(oSlab);
	}

	LOGGER_INFO("#" << __LINE__ << ", SlabMMapManager::SlabMMapManager, Done allocate swap: " << memory_size);
}

SlabMMapFactory::~SlabMMapFactory() {

	LOGGER_INFO("#" << __LINE__ << ", SlabMMapFactory::~SlabMMapFactory, Start deallocate swap: " << memory_size);
	oSlabManagers.clear();
	LOGGER_INFO("#" << __LINE__ << ", SlabMMapFactory::~SlabMMapFactory, Done deallocate swap: " << memory_size);
}

void SlabMMapFactory::stop() {
	for (const auto & oSlabManager : oSlabManagers) {
		oSlabManager->stop();
	}
}

/**
 * 从 各资源池 轮询 分配一个 块缓存，状态变为 正在使用，当 资源不够 时候，自动调用 GC 回收后再分配
 *
 * 分配新内存，特殊情况下，可能没有获取 块对象
 */
std::shared_ptr<SlabBlock> SlabMMapFactory::New() {
	size_t nSlabs = oSlabManagers.size();
	std::shared_ptr<SlabBlock> oSlabBlock;

	if (nSlabs == 0) {
		return oSlabBlock;
	}

	for (size_t iSlab = 0; iSlab < nSlabs; iSlab++) {
		size_t idx = iSlabIndex;
		if (idx >= nSlabs) {
			iSlabIndex = idx = 0;
		}
		iSlabIndex.fetch_add(1);

		oSlabBlock = oSlabManagers[idx]->New();
		if (oSlabBlock.get() != NULL) {
			return oSlabBlock;
		}
	}

	return oSlabBlock;
}

size_t SlabMMapFactory::GetReadMemBlocks() const {
	return oSlabMemFactory->GetReadMemBlocks();
}

size_t SlabMMapFactory::GetFreeMemBlocks() const {
	return oSlabMemFactory->GetFreeMemBlocks();
}

size_t SlabMMapFactory::GetWriteMemBlocks() const {
	return oSlabMemFactory->GetWriteMemBlocks();
}

size_t SlabMMapFactory::GetReadSwapBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMMapManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumReadBlocks();
	}
	return nBlocks;
}

size_t SlabMMapFactory::GetWriteSwapBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMMapManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumWriteBlocks();
	}
	return nBlocks;
}

size_t SlabMMapFactory::GetFreeSwapBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMMapManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumFreeBlocks();
	}
	return nBlocks;
}
