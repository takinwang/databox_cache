#include "SlabMemManager.hpp"

#include <databox/cpl_memdog.hpp>
INIT_IG(SlabMemBlock_watchdog, "SlabMemBlock");

SlabMemBlock::SlabMemBlock(SlabMemManager * manager_, char * buffer_, size_t slab_id_, size_t block_id_) :
		SlabBlock::SlabBlock(slab_id_, block_id_), pManager(manager_) {
	if (buffer_) {
		pBuffer = buffer_;
	} else {
		pBuffer = (char *) malloc( SIZEOFBLOCK);
	}

	bzero(pBuffer, SIZEOFBLOCK); //内存需要清 0
//	LOGGER_TRACE(
//			"#" << __LINE__ << ", SlabMemBlock::SlabMemBlock: " << slab_id << "-" << block_id << ", " << ( long ) this);

	INC_IG(SlabMemBlock_watchdog);
}

SlabMemBlock::~SlabMemBlock() {
	DEC_IG(SlabMemBlock_watchdog);

//	LOGGER_TRACE(
//			"#" << __LINE__ << ", SlabMemBlock::~SlabMemBlock: " << slab_id << "-" << block_id << ", " << ( long ) this);

	if (pBuffer) {
		free(pBuffer);
	}

	pBuffer = NULL;
	pManager = NULL;
}

/**
 * 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
 *  同时将块缓存放入到正在使用队列中
 * 多线程安全
 */
void SlabMemBlock::Commit() {
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
std::shared_ptr<SlabBlock> SlabMemBlock::Free() {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) {        //可能 Free 已经调用，将自己 gc 或 free 了
		return std::shared_ptr<SlabBlock>();
	}

	std::shared_ptr<SlabMemBlock> oSlabMemBlock = std::make_shared<SlabMemBlock>(pManager, pBuffer, slab_id, block_id);
	pBuffer = NULL;

	std::shared_ptr<SlabBlock> oNewSlabBlock = std::static_pointer_cast<SlabBlock, SlabMemBlock>(oSlabMemBlock);
	pManager->Free(oNewSlabBlock);

	version = 0;
	GC_Callback = nullptr;
	pManager = NULL;

	return oNewSlabBlock;
}

void SlabMemBlock::SetEditable(bool t) {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) {        //可能 Free 已经调用，将自己 gc 或 free 了
		return;
	}

	this->bEditable = t;

	auto self = this->shared_from_this();
	pManager->SwitchEditable(self);
}

/**
 * 在内存块上读取数据，version 不会变化
 * ptr_dst 待读取的数据缓存
 * size 待读取的数据大小
 * offset 待读取缓存区的位置偏移，小于 0，则为 0
 * 返回成功读取数据量，< 0 表示 offset > buffer_size
 *
 * 存在多线程调用，加锁
 */
int SlabMemBlock::Read( int offset, size_t size, std::string & ptr_dst) {
	if (size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	if (offset < 0) {
		offset = 0;
	}

	size_t uoff = offset;
	if (uoff >= used_size) { //没有空间可读
		return 0;
	}

	ptr_dst.clear();
	int toread = std::min<size_t>(size, used_size - uoff);

	if (toread > 0) {
		char * ptr_src = this->pBuffer + uoff;
		ptr_dst.assign(ptr_src, toread);
	}

	return toread;
}

/**
 * 存在多线程调用，加锁
 */
int SlabMemBlock::Read(uint64_t offset, uint32_t block_offset_id, size_t data_size, std::string & ptr_dst) {

	if (data_size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	uint32_t block0_offset_id = offset / SIZEOFBLOCK;

	int slab_offset = offset - (block0_offset_id * SIZEOFBLOCK); //第一块内存块中偏移
	int slab_length = std::min<int>(data_size, SIZEOFBLOCK - slab_offset); //第一块内存块中剩余长度

	if (block_offset_id > block0_offset_id) { //第一块以后
		slab_offset = 0; //其他内存块中偏移
		int buff_offset = slab_length + (block_offset_id - block0_offset_id - 1) * SIZEOFBLOCK; //第一块内存块中剩余长度 + 之间完整块长度
		slab_length = std::min<int>(data_size - buff_offset, SIZEOFBLOCK);
	}

	if (slab_offset < 0 || slab_length <= 0) {
		return 0;
	}

	if (static_cast<size_t>(slab_offset) >= this->used_size) {
		return 0;
	}

	ptr_dst.clear();
	int data_length = std::min<int>(this->used_size - slab_offset, slab_length); // 内存数据剩余长度

	if (data_length > 0) {
		char * ptr_src = this->pBuffer + slab_offset;
		ptr_dst.assign(ptr_src, data_length);
	}

	return data_length;
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
int SlabMemBlock::WriteBlock(const char* ptr_src, size_t size, int offset) {
	if (size <= 0 || ptr_src == NULL) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	if (offset < 0) { // 小于 0，则为 append 模式
		offset = this->used_size;
	}

	size_t uoff = offset;
	if (uoff >= SIZEOFBLOCK) { //没有空间写入
		return 0;
	}

	int towrite = std::min<size_t>(size, SIZEOFBLOCK - uoff);
	if (towrite > 0) {
		char * ptr_dst = this->pBuffer + uoff;

		memcpy(ptr_dst, ptr_src, towrite);
		this->used_size = std::max<size_t>(this->used_size, uoff + towrite);
	}

	return towrite;
}

/**
 * 二次修改数据的时候，在内存块上写入数据，version++，dirty为true，是修改数据！！！
 * 存在多线程调用，加锁
 */
int SlabMemBlock::Write(uint64_t offset, uint32_t block_offset_id, const std::string & data) {
	size_t data_size = data.size();
	if (data_size <= 0) {
		return 0;
	}

	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (this->IsValid() == false) { //  可能 Clone 已经调用，将自己 gc 或 free 了
		return -1;
	}

	const char * data_ptr = data.c_str();

	uint32_t block0_offset_id = offset / SIZEOFBLOCK;

	int slab_offset = offset - (block0_offset_id * SIZEOFBLOCK); //第一块内存块中偏移
	int slab_length = std::min<int>(data_size, SIZEOFBLOCK - slab_offset); //第一块内存块中剩余长度
	int buff_offset = 0; //第一块源数据中的偏移位置

	if (block_offset_id > block0_offset_id) { //第一块以后
		slab_offset = 0; //其他内存块中偏移
		buff_offset = slab_length + (block_offset_id - block0_offset_id - 1) * SIZEOFBLOCK; //第一块内存块中剩余长度 + 之间完整块长度
		slab_length = std::min<int>(data_size - buff_offset, SIZEOFBLOCK);
	}

	if (slab_offset < 0 || slab_length <= 0) {
		return 0;
	}

	char * ptr_dst = this->pBuffer + slab_offset;
	const char * ptr_src = data_ptr + buff_offset;

	memcpy(ptr_dst, ptr_src, slab_length);

	this->used_size = std::max<size_t>(this->used_size, slab_offset + slab_length);

	this->version++;
//	this->bDirty = true;

	return slab_length;
}

void SlabMemBlock::SetCallback(
		const std::function<void(const std::shared_ptr<SlabMemBlock> & oSlabBlock)> & callback_) {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	this->GC_Callback = callback_;
}

void SlabMemBlock::Callback(const std::shared_ptr<SlabMemBlock> & oSlabBlock) {
	std::lock_guard<std::mutex> lock(mtx); //防止多线读写冲突
	if (GC_Callback) {
		GC_Callback(oSlabBlock);
	}
}

SlabMemManager::SlabMemManager(size_t SlabId_) :
		slab_id(SlabId_), read_blocks_map(NUMBLOCKS * 2), write_blocks_map( NUMBLOCKS * 2) {

	char * buffer = NULL;
	for (size_t block_id = 0; block_id < NUMBLOCKS; block_id++) {
		std::shared_ptr<SlabMemBlock> oSlabMemBlock = std::make_shared<SlabMemBlock>(this, buffer, slab_id, block_id);
		std::shared_ptr<SlabBlock> oNewSlabBlock = std::static_pointer_cast<SlabBlock, SlabMemBlock>(oSlabMemBlock);

		idle_blocks_queue.push_back(oNewSlabBlock);
	}

}

SlabMemManager::~SlabMemManager() {
	idle_blocks_queue.clear();
	read_blocks_map.clear();
	write_blocks_map.clear();
}

/**
 * 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
 *  同时将块缓存放入到正在使用队列中
 * 多线程安全
 */
void SlabMemManager::Commit(const std::shared_ptr<SlabBlock> & oSlabBlock) {
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
			"#" << __LINE__ << ", SlabMemManager::Commit, SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() //
			<< ", Queue: " << idle_blocks_queue.qsize() << ", Used for Read: " << read_blocks_map.size() << ", Used for Write: " << write_blocks_map.size());
}

/**
 * 释放当前 块缓存 到 资源池，不管是 只读或读写模式，都被 Free
 * 被 SlabMemBlock.Free 调用
 * 多线程安全
 */
void SlabMemManager::Free(const std::shared_ptr<SlabBlock> & oNewSlabBlock) {
	size_t block_id = oNewSlabBlock->GetBlockId();

	{
		std::lock_guard<std::mutex> lock(mtx);
//		为了保障 read_blocks_map 和 write_blocks_map 一致性，需要加锁
		read_blocks_map.erase(block_id);
		write_blocks_map.erase(block_id);
	}

	idle_blocks_queue.push_back(oNewSlabBlock);
	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabMemManager::Free, SlabId: " << oNewSlabBlock->GetSlabId() << ", BlockId: " << block_id << ", Queue: " << idle_blocks_queue.qsize());

}

/**
 * 将当前块变为修改模式或只读模式，修改模式下可以避免被 GC 释放
 */
std::shared_ptr<SlabBlock> SlabMemManager::SwitchEditable(const std::shared_ptr<SlabBlock>& oSlabBlock) {
	std::lock_guard<std::mutex> lock(mtx);
	bool t = oSlabBlock->IsEditable();

	size_t block_id = oSlabBlock->GetBlockId();
	if (t == true) {
		read_blocks_map.erase(block_id);
		write_blocks_map.put(block_id, oSlabBlock, 0);
	} else {
		write_blocks_map.erase(block_id);
		read_blocks_map.put(block_id, oSlabBlock, 0);
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", SlabMemManager::SwitchEditable, SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() //
			<< ", Queue: " << idle_blocks_queue.qsize() << ", Used for Read: " << read_blocks_map.size() << ", Used for Write: " << write_blocks_map.size());

	return oSlabBlock;
}

/**
 * 从 资源池 分配一个 块缓存，状态变为 正在使用
 * 由 New 获取的块对象，必须调用 Commit 或 Free 确保内存块不丢失
 * 多线程调用，加锁顺序分配，由于上级是轮询方式，所以性能问题不大
 * 正常情况下，GC 会从 只读块队列中释放空白块，当只读块队列没有话，会得不到内存块
 *
 */
std::shared_ptr<SlabBlock> SlabMemManager::New() {
	std::shared_ptr<SlabBlock> oSlabBlock;
	if (idle_blocks_queue.pop_front(oSlabBlock) == true && oSlabBlock.get() != NULL) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMemManager::New, Idle SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() //
				<< ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
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
				"#" << __LINE__ << ", SlabMemManager::New, Blank Slab, Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
		return oNullBlock;
	}

	//GC 回调处理，如果关联了 swap，需要 swap 同步数据，同时取消关联

	std::shared_ptr<SlabMemBlock> oSlabMemBlock = std::static_pointer_cast<SlabMemBlock, SlabBlock>(oSlabBlock);
	oSlabMemBlock->Callback(oSlabMemBlock);

	/** 由于原有对象被其他引用，需要 Clone 一个新对象重新放入到队列，原有对象 version = 0，buffer = NULL 状态变为不可用
	 * Clone 已经加锁保障，重复调用，后续返回 空 对象
	 */

	oSlabBlock->Free();
	oSlabBlock.reset();

	if (idle_blocks_queue.pop_front(oSlabBlock) == true && oSlabBlock.get() != NULL) {
		LOGGER_TRACE(
				"#" << __LINE__ << ", SlabMemManager::New, Idle SlabId: " << oSlabBlock->GetSlabId() << ", BlockId: " << oSlabBlock->GetBlockId() //
				<< ", Queue: " << idle_blocks_queue.qsize() << ", Used: " << read_blocks_map.size());
		return oSlabBlock;
	}

	return oSlabBlock;
}

SlabMemFactory::SlabMemFactory(size_t maxSlabs_) {

	memory_size = StringUtils::FormatBytes(static_cast<uint64_t>(maxSlabs_) * SIZEOFBLOCK * NUMBLOCKS);
	LOGGER_INFO("#" << __LINE__ << ", SlabMemFactory::SlabMemFactory, Start allocate memory: " << memory_size);

	for (size_t slab_id = 0; slab_id < maxSlabs_; slab_id++) {
		std::shared_ptr<SlabMemManager> oSlabMemManager = std::make_shared<SlabMemManager>(slab_id);

		if (oSlabMemManager.get() == NULL) { //没有内存了
			oSlabManagers.clear();
			std::stringstream ss;
			ss << "No Empty Memory, Requires at least: " << memory_size;
			throw std::runtime_error(ss.str());
		}

		oSlabManagers.push_back(oSlabMemManager);
	}

	LOGGER_INFO("#" << __LINE__ << ", SlabMemFactory::SlabMemFactory, Done allocate memory: " << memory_size);
}

SlabMemFactory::~SlabMemFactory() {
	LOGGER_INFO("#" << __LINE__ << ", SlabMemFactory::~SlabMemFactory, Start deallocate memory: " << memory_size);
	oSlabManagers.clear();
	LOGGER_INFO("#" << __LINE__ << ", SlabMemFactory::~SlabMemFactory, Done deallocate memory: " << memory_size);
}

void SlabMemFactory::stop() {
}

/**
 * 从 各资源池 轮询 分配一个 块缓存，状态变为 正在使用，当 资源不够 时候，自动调用 GC 回收后再分配
 *
 * 分配新内存，特殊情况下，可能没有获取 块对象
 */
std::shared_ptr<SlabBlock> SlabMemFactory::New() {
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

size_t SlabMemFactory::GetReadMemBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMemManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumReadBlocks();
	}
	return nBlocks;
}

size_t SlabMemFactory::GetWriteMemBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMemManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumWriteBlocks();
	}
	return nBlocks;
}

size_t SlabMemFactory::GetFreeMemBlocks() const {
	size_t nBlocks = 0;
	for (const std::shared_ptr<SlabMemManager> & oSlabBlock : oSlabManagers) {
		nBlocks += oSlabBlock->GetNumFreeBlocks();
	}
	return nBlocks;
}

size_t SlabMemFactory::GetReadSwapBlocks() const {
	return 0;
}

size_t SlabMemFactory::GetWriteSwapBlocks() const {
	return 0;
}

size_t SlabMemFactory::GetFreeSwapBlocks() const {
	return 0;
}

