/*
 * SlabForward.hpp
 *
 *  Created on: Nov 5, 2018
 *      Author: root
 */

#ifndef MEMORY_SLABFORWARD_HPP_
#define MEMORY_SLABFORWARD_HPP_

#include <functional>
#include <mutex>
#include <memory>

// 默认管理 1024 * 1 内存块，每块 256 KB，共 256 MB
// 对应 CacheClient.hpp 里面需要一致

#define SIZEOFBLOCK ( 256 * 1024 )

//#define SIZEOFBLOCK ( 128 )

#define NUMBLOCKS   ( 1024 )

class SlabBlock: public std::enable_shared_from_this<SlabBlock> {
public:
	SlabBlock(size_t slab_id_, size_t block_id_) :
			slab_id(slab_id_), block_id(block_id_) {
	}

	virtual ~SlabBlock() {
	}

	/**
	 * 手动提交 块缓存 状态变为 正在使用，确保块处于使用中，不改变块状态
	 *  同时将块缓存放入到正在使用队列中
	 * 多线程安全
	 */
	virtual void Commit() = 0;

	/**
	 * 释放当前 块缓存 到 资源池，不管是 只读或读写模式，都被 Free，clone 一个新对象重新放入到队列
	 * 多线程安全
	 */
	virtual std::shared_ptr<SlabBlock> Free() = 0;

	/**
	 * 初次加载数据的时候，在内存块上写入数据，version 不变，dirty 不变，不是修改数据！！！
	 * ptr 待写入的数据位置，数据将从头读取
	 * size 待写入的数据大小
	 * offset 待写入缓存区的位置偏移，小于 0，则为 append 模式，从数据缓冲区最后开始写入
	 * 返回成功写入数据量，< 0 表示 offset > buffer_size
	 *
	 * 内部加锁，多线程安全
	 */
	virtual int WriteBlock(const char* ptr_src, size_t size, int offset) = 0;
	/**
	 * 二次修改数据的时候，在内存块上写入数据，version++，dirty为true，是修改数据！！！
	 * 多线程安全
	 */
	virtual int Write(uint64_t offset, uint32_t block_offset_id, const std::string & data) = 0;
	/**
	 * 在内存块上读取数据，version 不会变化
	 * ptr_dst 待读取的数据缓存
	 * size 待读取的数据大小
	 * offset 待读取缓存区的位置偏移，小于 0，则为 0
	 * 返回成功读取数据量，< 0 表示 offset > buffer_size
	 *
	 * 内部加锁，多线程安全
	 *
	 * 不能返回指针的指针，脱离该函数后获取数据，无法保证线程安全，只能采用值拷贝返回 std::string
	 */
	virtual int Read(int offset, size_t size, std::string & ptr_dst) = 0;
	/**
	 * 读取数据，根据文件偏移位置和当前块在文件中的偏移编号来读取数据
	 *
	 * 内部加锁，多线程安全
	 *
	 * 不能返回指针的指针，脱离该函数后获取数据，无法保证线程安全，只能采用值拷贝返回 std::string
	 */
	virtual int Read(uint64_t offset, uint32_t block_offset_id, size_t size, std::string & ptr_dst) = 0;

//	void ClearDirty() {
//		this->bDirty = false;
//	}

	/**
	 * 数据占用空间大小
	 */
	ssize_t GetUsedSize() const {
		if (this->version < 1) {
			return 0;
		}
		return this->used_size;
	}

//当前数据版本，存在多线程调用
	const int32_t GetVersion() const {
		return this->version;
	}

	/**
	 * 内存块数据 与 元数据端 新旧关闭比对方法
	 * 优先通过 版本 比对，如果两则 版本一致，比较两者 的 创建时间，创建时间新的 为新数据
	 * 存在多线程调用
	 */

	virtual void SetVersion(const int32_t version) {
		this->version = version;
	}

	size_t GetBlockId() const {
		return this->block_id;
	}

	size_t GetSlabId() const {
		return this->slab_id;
	}

	virtual const bool IsValid() = 0;

//	const bool IsDirty() {
//		return this->bDirty;
//	}

	const bool IsEditable() const {
		return this->bEditable;
	}

	virtual void SetEditable(bool t) = 0;

protected:
//	std::atomic<bool> bDirty { false }; //是否数据被修改
	std::atomic<bool> bEditable { false }; //是否需要修改

	size_t slab_id { 0 }; //当前 slab 的索引
	size_t block_id { 0 }; //当前 block 的索引

	size_t used_size { 0 }; //当前块的使用大小
	std::atomic<int32_t> version { 1 }; //存在多线程设置该值，需要保证原子操作

	std::mutex mtx;
};

class SlabFactory {
public:
	virtual ~SlabFactory() {
	}

	virtual void stop() = 0;

	/**
	 * 从 资源池 分配一个 块缓存，状态变为 正在使用
	 * 由 New 获取的块对象，必须调用 Commit 或 Free 确保内存块不丢失
	 * 多线程调用，加锁顺序分配，由于上级是轮询方式，所以性能问题不大
	 * 正常情况下，GC 会从 只读块队列中释放空白块，当只读块队列没有话，会得不到内存块
	 * 多线程安全
	 *
	 */
	virtual std::shared_ptr<SlabBlock> New() = 0;

	virtual size_t GetReadMemBlocks() const = 0;

	virtual size_t GetFreeMemBlocks() const = 0;

	virtual size_t GetWriteMemBlocks() const = 0;

	virtual size_t GetReadSwapBlocks() const = 0;

	virtual size_t GetFreeSwapBlocks() const = 0;

	virtual size_t GetWriteSwapBlocks() const = 0;

};

#endif /* MEMORY_SLABFORWARD_HPP_ */
