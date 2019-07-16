/*
 * CacheClient.hpp
 *
 *  Created on: Oct 21, 2018
 *      Author: root
 */

#ifndef CACHECLIENT_HPP_
#define CACHECLIENT_HPP_
#include <memory>
#include <mutex>
#include <future>         // std::promise, std::future

#include <databox/stringutils.hpp>
#include <databox/stringbuffer.hpp>

#include <databox/hbclient.hpp>
#include <databox/hbserver.hpp>

#include <databox/async_messager.hpp>

#include <databox/lrucache.hpp>

#include <databox/cpl_debug.h>

#define	tsFailed   -1000 //失败
#define	tsTimeout  -1001 //超时
#define	tsSuccess   0 //成功

#define MAX_REQUEST_SIZE ( 8 * 1024 * 1024 ) //单次请求的最大数据长度

// 默认管理 1024 * 1 内存块，每块 256 KB，共 256 MB
// 对应 SlabForward.hpp 里面需要一致
#define SIZEOFBLOCK ( 256 * 1024 )

//#define SIZEOFBLOCK ( 128 )

#define FAILED_INVALID_MEMORY      "Invalid memory"
#define FAILED_INVALID_ARGUMENT    "Invalid argument"
#define FAILED_INVALID_RESPONSE    "Invalid response"
#define FAILED_INTERNAL_ERROR      "Internal Error"
#define FAILED_CONNECTION_TIMEOUT  "Connection timeout"
#define FAILED_CONNECTION_FAILED   "Connection failed"

#define MEM_PREFIX   "mem://" //内存文件前缀

#define TIMEOUT 30

//Distributed Virtual Buffer
//分布式虚拟缓存
namespace DVB {

enum CacheAction {
	caSuccess = 0,             //通用返回

	caOpen = 1,                //打开缓存文件
	caClose = 2,               //关闭缓存文件

	caClientRead2 = 9,         //并行读取缓存数据，支持 c++ 库
	caClientRead = 10,         //读取缓存数据
	caClientReadResp = 11,     //读取数据返回

	caClientWrite = 12,        //写入数据到缓存
	caClientWriteResp = 13,    //写入数据返回

	caClientUnlink = 14,       //客户端删除文件
	caClientUnlinkResp = 15,   //客户端删除文件返回

	caClientMkDir = 16,        //客户端建立目录
	caClientMkDirResp = 17,    //客户端建立目录返回

	caClientRmDir = 18,        //客户端删除目录
	caClientRmDirResp = 19,    //客户端删除目录返回

	caClientTruncate = 20,     //客户端截取文件
	caClientTruncateResp = 21, //客户端截取文件返回

	caClientGetAttr = 22,      //客户端读取文件属性
	caClientGetAttrResp = 23,  //客户端读取文件属性返回

	caClientFlush = 24,        //客户端刷新写入数据
	caClientFlushResp = 25     //客户端刷新写入数据返回

};

struct FileStat {
	time_t mtime { 0 };
	off_t size { 0 };

	FileStat() {
	}

	FileStat(uint64_t size_, time_t mtim_) :
			mtime(mtim_), size(size_) {
	}
};

class CachedFiles {
public:
	struct CachedData {
		std::string value;
	};

	class CachedChunks {
	public:
		cache::lru_cache_count_num<int32_t, std::shared_ptr<CachedData> > Chunks;

		CachedChunks() :
				Chunks(256) {
		}
	};

	cache::lru_cache_count_num<std::string, std::shared_ptr<CachedChunks> > oCachedFiles;

public:
	CachedFiles() :
			oCachedFiles(256) {
	}

	void put(const std::string& file, int32_t block_id, const std::string & value);

	bool get(const std::string& file, int32_t block_id, std::shared_ptr<CachedData> & data);

	void erase(const std::string& file);

	void erase(const std::string& file, int32_t block_id);

};

class DboxSlabClient;

class VFile {
public:
	VFile(const std::shared_ptr<DboxSlabClient> & oCacheClient, const std::shared_ptr<BaseMsgSender>& oMsgSender,
			const std::string& filename, const std::string & mode, int timeout = TIMEOUT);

	/**
	 * 服务端串行读取数据
	 */
	ssize_t Read(void * buffer, size_t buffer_size, off_t offset);

	/**
	 * 客户端采用分块并行读取
	 */
	ssize_t Read2(void * buffer, size_t buffer_size, off_t offset);

	ssize_t Write(const void * buffer, size_t buffer_size, off_t offset, bool async_write = true);

	/**
	 * 刷新远程块缓存服务器的修改块 和 后端存储
	 */
	int Flush();

	int Truncate(off_t newsize);

	int GetAttr(struct FileStat &stat);

	const std::string& getFilename() const {
		return msFilename;
	}

	const std::string& getMessage() const {
		return msMessage;
	}

private:
	std::shared_ptr<DboxSlabClient> moFileClient;
	std::shared_ptr<BaseMsgSender> moMsgSender;

	std::string msFilename;
	std::string msMessage;

	std::string msMode;
	int miTimeout;

	bool bReadOnly { false }; /** 只读模式 */
//	bool bAppend { false }; /** 追加读写 */

//	off_t mOffset { 0 }; /** 读写偏移 */
};

class DboxSlabClient: public std::enable_shared_from_this<DboxSlabClient> {
public:

	static CachedFiles oCachedFiles;

	/**
	 * 初始化内部工作线程，不调用将不可用
	 */

	static void initialize(uint32_t qsize);

	/**
	 * 退出内部工作线程，调用后将不可用
	 */

	static void finalize();

public:

	DboxSlabClient(const std::string & hostinfo_, int timeout_ = TIMEOUT);

	~DboxSlabClient();

	/**
	 新建一个文件对象进行读写

	 mode: r 只读模式，w 修改模式

	 当 readonly == true  模式，由于使用属性（size、version等）缓存优化 3s，读取效率更高，但当其他块缓存服务器上写入数据的后端存储文件，被当前节点在写入之前已经访问，可能会存在数据不同步问题
	 当 readonly == false 无数据不同步问题

	 或则先 Close 当前块缓存服务器上 filename 的缓存，再访问
	 */
	std::shared_ptr<VFile> Open(const std::string & filename, const std::string & mode);

	/**
	 * 在远程块缓存服务器 和 后端存储上删除文件，不同后端存储不一定都实现，其他远程块缓存服务器可能会存在60s的缓存时间
	 */
	int Unlink(const std::string & filename, std::string & message);

	/**
	 * 在后端存储上建立目录 filename，不同后端存储不一定都实现
	 */
	int MkDir(const std::string & filename, std::string & message);

	/**
	 * 在后端存储上删除目录 filename，不同后端存储不一定都实现
	 */
	int RmDir(const std::string & filename, std::string & message);

	/**
	 * 在远程块缓存服务器 和 后端存储上文件属性（ mtime 和 size ），可能会存在60s的缓存时间
	 */
	int GetAttr(const std::string & filename, struct FileStat &stat, std::string & message);

	/**
	 * 在 后端存储上截取文件，不同后端存储不一定都实现，其他远程块缓存服务器可能会存在60s的缓存时间
	 * 对于 mem:// 文件，暂时未实现
	 */
	int Truncate(const std::string & filename, off_t newsize, std::string & message);

	/**
	 * 关闭远程块缓存服务器上的 filename 缓存信息，再次访问会从邻居或后端存储重新加载
	 * 对于 mem:// 文件，无效
	 */
	int Close(const std::string & filename, std::string & message);

private:
	std::string msHostinfo;
	int miTimeout { TIMEOUT };

	std::shared_ptr<BaseMsgSender> moMsgSender;
	std::shared_ptr<AsyncIOService> moAsyncIOService;
};

}
;

#endif /* CACHECLIENT_HPP_ */
