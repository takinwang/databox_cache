/*
 * SlabMMapManager.hpp
 *
 *  Created on: Oct 12, 2018
 *      Author: root
 */

#ifndef SLABMMAPMANAGER_HPP_
#define SLABMMAPMANAGER_HPP_

#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdexcept>

#include <atomic>

#include <math.h>
#include <list>
#include <set>
#include <unordered_map>

#include <databox/cpl_debug.h>
#include <databox/lrucache.hpp>

#include <databox/stringbuffer.hpp>
#include <databox/stringutils.hpp>
#include <databox/filesystemutils.hpp>

#include "SlabForward.hpp"
#include "SlabMemManager.hpp"

class dbox_error: public std::logic_error {
public:
	dbox_error(const std::string& __arg) :
			std::logic_error(__arg) {
	}
};

class SlabMMapManager;

/**
 * 硬盘映射数据块，通常与SlabBlock具有相同大小，当内存没有数据块，从当前磁盘块读取数据放入内存块
 * 当内存块被gc时候，将内存块数据写入数据块，当磁盘块不够时候，需要gc
 */
class SlabMMapBlock: public SlabBlock {
public:
	friend class SlabMemBlock;
	friend class SlabMMapManager;
	friend class SlabMMapFactory;
public:
	/**
	 * 没有进行 0 填充，由于写入有些是写入到内存缓存上，内存缓存不够的时候，将整个内存块 copy 到 swap 缓存，不会出现垃圾数据
	 */
	SlabMMapBlock(SlabMMapManager * pManager_, SlabMemFactory *pMemFactory_, int fd_, size_t slab_id_,
			size_t block_id_);

	~SlabMMapBlock();

	/**
	 * 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
	 *  同时将块缓存放入到正在使用队列中
	 * 多线程安全
	 */
	void Commit();

	/**
	 * 释放当前 块缓存 到 资源池，不管是 只读或读写模式，都被 Free，clone 一个新对象重新放入到队列
	 * 多线程安全
	 */
	std::shared_ptr<SlabBlock> Free();

//	//被垃圾回收后，version 为 0，后续不能继续使用，当前对象在无引用下自动 free，需要 clone 一个新对象重新放入到队列
//	std::shared_ptr<SlabBlock> Clone();

	/**
	 * 在内存块上写入数据，version 不会变化
	 * ptr_src 待写入的数据位置，数据将从头读取
	 * size 待写入的数据大小
	 * offset 待写入缓存区的位置偏移，小于 0，则为 append 模式，从最后位置开始写入
	 * 返回成功写入数据量，< 0 表示 offset > buffer_size
	 */
	int WriteBlock(const char* ptr_src, size_t size, int offset);

	/**
	 * 根据数据序列的偏移位置和当前块在数据序列的偏移编号来写入数据
	 * offset 数据序列的偏移位置
	 * block_offset_id 当前块在数据序列的偏移编号
	 * data 待写入的数据
	 */
	int Write(uint64_t offset, uint32_t block_offset_id, const std::string & data);

	/**
	 * 在内存块上读取数据，version 不会变化
	 * ptr_dst 待读取的数据缓存
	 * size 待读取的数据大小
	 * offset 待读取缓存区的位置偏移，小于 0，则为 0
	 * 返回成功读取数据量，< 0 表示 offset > buffer_size
	 * 存在多线程写入和读写情况，加锁
	 * 不能返回指针的指针，脱离该函数后获取数据，无法保证线程安全，只能采用值拷贝返回 std::string
	 */
	int Read( int offset,size_t size, std::string & ptr_dst);
	/**
	 * 读取数据，根据文件偏移位置和当前块在文件中的偏移编号来读取数据
	 * 存在多线程写入和读写情况，加锁
	 *  不能返回指针的指针，脱离该函数后获取数据，无法保证线程安全，只能采用值拷贝返回 std::string
	 */
	int Read(uint64_t offset, uint32_t block_offset_id, size_t size, std::string & ptr_dst);
	//	 int Read(uint64_t offset, uint32_t block_offset_id, size_t size, char ** buffer_ptr) ;

	void SetVersion(const int32_t version);

	const bool IsValid() {
		return (pManager != NULL) && (fd >= 0) && (version > 0);
	}

	void SetEditable(bool t);

private:

	/**
	 * 通过内存映射方式写入数据，最多写入 min(size, SIZEOFBLOCK) 数据
	 */
	int WriteFromBuffer(const char * buffer, size_t size);

	/**
	 * 通过内存映射方式读取数据，最多读取 min(size, SIZEOFBLOCK) 数据
	 */
	int ReadToBuffer(char * buffer, size_t size);

	std::shared_ptr<SlabBlock> CheckForIO();

	void ResetSlabMem();
private:
	int fd { -1 };

	std::shared_ptr<SlabMemBlock> oSlabMemBlock;
	SlabMemFactory *pMemFactory { NULL };
	SlabMMapManager * pManager { NULL };
};

class SlabMMapManager {
public:
	friend class SlabMMapBlock;

protected:
	/**
	 * 释放当前数据块到资源池
	 */
	void Free(const std::shared_ptr<SlabBlock>& oNewSlabBlock);

	/** 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
	 *  同时将块缓存放入到正在使用队列中
	 */
	void Commit(const std::shared_ptr<SlabBlock>& oSlabBlock);

	/**
	 * 将当前块变为修改模式，从而避免自动被 GC 释放，但可以手动调用 Free 释放，可以手动调用 SwitchReadOnly( ) 进入只读队列
	 */
	std::shared_ptr<SlabBlock> SwitchEditable(const std::shared_ptr<SlabBlock> & oSlabBlock);

public:
	SlabMMapManager(const std::shared_ptr<SlabMemFactory>& oSlabMemFactory_, const std::string & root, size_t SlabId_);

	~SlabMMapManager();

	void Close();

	void stop() {
		bTerminating = true;
	}

	/**
	 * 从 资源池 分配一个 块缓存，当 资源池的资源不够 时候，自动调用 GC 回收后再分配
	 * 需要手动调用 Commit 变为 正在使用，若不调用，则该块变为游离状态
	 */
	std::shared_ptr<SlabBlock> New();

public:
	const bool Terminated() const {
		return bTerminating.load();
	}

	bool IsValid() const {
		return fd >= 0;
	}

	size_t GetNumBlocks() {
		return NUMBLOCKS;
	}

	size_t GetNumWriteBlocks() {
		return write_blocks_map.size();
	}

	size_t GetNumReadBlocks() {
		return read_blocks_map.size();
	}

	size_t GetNumFreeBlocks() {
		return idle_blocks_queue.qsize();
	}

private:
	std::shared_ptr<SlabMemFactory> oSlabMemFactory;

	int fd { -1 };
	std::string filename;

	size_t slab_id;
	std::mutex mtx;

	std::atomic<bool> bTerminating { false };

	/**
	 * 正在读取使用中的 块缓存，为 LRUCache 缓存，定期调用 gc 进行内存回收
	 */
	cache::lru_cache_count_num<size_t, std::shared_ptr<SlabBlock> > read_blocks_map;

	/**
	 * 正在写入使用中的 块缓存，为 LRUCache 缓存，当数据写入完成后，需要迁移到 read_blocks_map
	 * 写入缓存不会被 gc 调用，限定总数为可用块数 1/2
	 */
	cache::lru_cache_count_num<size_t, std::shared_ptr<SlabBlock> > write_blocks_map;

	/**
	 * 空闲可用的 块缓存，由 Free() 回收的数据
	 */
	mtsafe::thread_safe_queue<std::shared_ptr<SlabBlock> > idle_blocks_queue;
};

class SlabMMapFactory: public SlabFactory {
private:
	std::shared_ptr<SlabMemFactory> oSlabMemFactory;
	std::vector<std::shared_ptr<SlabMMapManager> > oSlabManagers;
	std::atomic_int iSlabIndex { 0 }; //供 New 的时候进行轮询
	std::string memory_size;
public:

	/**
	 * 新建内存存储块
	 */
	SlabMMapFactory(const std::shared_ptr<SlabMemFactory> & oSlabMemFactory_, const std::string & root,
			size_t maxSlabs_ = 10);

	~SlabMMapFactory();

	void stop();

	/**
	 * 从 各资源池 轮询 分配一个 块缓存，状态变为 正在使用，当 资源不够 时候，自动调用 GC 回收后再分配
	 */
	std::shared_ptr<SlabBlock> New();

	size_t GetReadMemBlocks() const;

	size_t GetFreeMemBlocks() const;

	size_t GetWriteMemBlocks() const;

	size_t GetReadSwapBlocks() const;

	size_t GetFreeSwapBlocks() const;

	size_t GetWriteSwapBlocks() const;

};

#endif /* SLABMMAPMANAGER_HPP_ */
