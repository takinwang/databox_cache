/*
 * SlabMemManager.hpp
 *
 *  Created on: Oct 12, 2018
 *      Author: root
 */

#ifndef SLABMEMMANAGER_HPP_
#define SLABMEMMANAGER_HPP_

#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <atomic>

#include <math.h>
#include <list>
#include <set>
#include <unordered_map>

#include <databox/cpl_debug.h>
#include <databox/lrucache.hpp>

#include <databox/mtsafe_object.hpp>

#include <databox/stringutils.hpp>
#include <databox/filesystemutils.hpp>

#include "SlabForward.hpp"

class SlabMMapBlock;
class SlabMemManager;
class SlabMemFactory;

class SlabMemBlock: public SlabBlock {
	friend class SlabMMapBlock;
	friend class SlabMemManager;
	friend class SlabMemFactory;
public:
	SlabMemBlock(SlabMemManager * manager_, char * buffer_, size_t slab_id_, size_t block_id_);

	~SlabMemBlock();

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
	int Read(int offset, size_t size, std::string & ptr_dst);
	/**
	 * 读取数据，根据文件偏移位置和当前块在文件中的偏移编号来读取数据
	 * 存在多线程写入和读写情况，加锁
	 *  不能返回指针的指针，脱离该函数后获取数据，无法保证线程安全，只能采用值拷贝返回 std::string
	 */
	int Read(uint64_t offset, uint32_t block_offset_id, size_t size, std::string & ptr_dst);

	const bool IsValid() {
		return (pManager != NULL) && (pBuffer != NULL) && (version > 0);
	}

	void SetEditable(bool t);

	void SetCallback(const std::function<void(const std::shared_ptr<SlabMemBlock> & oSlabBlock)> & callback_);

	void Callback(const std::shared_ptr<SlabMemBlock> & oSlabBlock);

protected:
	//当前数据指针
	char * pBuffer { NULL };

	SlabMemManager * pManager;
	std::function<void(const std::shared_ptr<SlabMemBlock> & oSlabBlock)> GC_Callback { nullptr };

};

/**
 * 内存管理策略：
 *    采用线程安全的 LRUCache 管理正在使用的 块缓存，当内存不够时候，最不使用的尾部数据会被 GC 批量移除，移除后回到 管理空闲 队列
 *    采用线程安全的 Queue 管理空闲可用的 块缓存

 *    当客户端写入数据到 SlabBlock 时候，该 SlabBlock 的 version++，
 *    提交 SlabBlock 更改信息到元数据节点，由元数据节点推送该块其他节点进行删除该节点的 SlabBlock 数据
 *
 *    读取数据的时候，如果当前节点没有数据，从元数据节点查询块缓存分布情况
 *    通过比对 数据节点和元数据节点的 utime 和 version，确定数据是否被修改
 *    正常情况下，数据被修改时，修改节点和元数据节点的 utime 和 version 是一致的，其他节点的  utime 和 version 较小
 */
class SlabMemManager {
public:
	friend class SlabMemBlock;
protected:

	/**
	 * 释放当前 块缓存 到 资源池，不管是 只读或读写模式，都被 Free
	 * 被 SlabMemBlock.Free 调用
	 * 多线程安全
	 */

	void Free(const std::shared_ptr<SlabBlock> & oNewSlabBlock);

	/** 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
	 *  同时将块缓存放入到正在使用队列中
	 *  多线程安全
	 */
	void Commit(const std::shared_ptr<SlabBlock> & oSlabBlock);
	/**
	 * 将当前块变为修改模式，从而避免自动被 GC 释放，但可以手动调用 Free 释放，可以手动调用 SwitchReadOnly( ) 进入只读队列
	 */
	std::shared_ptr<SlabBlock> SwitchEditable(const std::shared_ptr<SlabBlock> & oSlabBlock);
public:
	/**
	 * 内部没有检查异常，如果内存不够 isValid() 返回 false
	 */
	SlabMemManager(size_t SlabId_);

	~SlabMemManager();

	/**
	 * 从 资源池 分配一个 块缓存，当 资源池的资源不够 时候，自动调用 GC 回收后再分配
	 * 需要手动调用 Commit 变为 正在使用，若不调用，则该块变为游离状态
	 */
	std::shared_ptr<SlabBlock> New();

public:

	size_t GetNumBlocks() const {
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

	size_t slab_id;

	std::mutex mtx;

	std::shared_ptr<SlabBlock> oNullBlock;

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

class SlabMemFactory: public SlabFactory {
private:
	std::vector<std::shared_ptr<SlabMemManager> > oSlabManagers;
	std::atomic_int iSlabIndex { 0 }; //供 New 的时候进行轮询
	std::string memory_size;
public:
	/**
	 * 新建内存存储块
	 */
	SlabMemFactory(size_t maxSlabs_ = 10);

	~SlabMemFactory();

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

#endif /* SLABMEMMANAGER_HPP_ */
