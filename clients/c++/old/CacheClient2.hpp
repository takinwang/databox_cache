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
#include <databox/lrucache.hpp>

#include <databox/cpl_debug.h>

#define	tsFailed   -1000 //失败
#define	tsTimeout  -1001 //超时
#define	tsSuccess   0 //成功

#define MAX_REQUEST_SIZE ( 8 * 1024 * 1024 ) //单次请求的最大数据长度

// 默认管理 1024 * 1 内存块，每块 256 KB，共 256 MB
// 对应 SlabForward.hpp 里面需要一致
#define SIZEOFBLOCK ( 256 * 1024 )

#define FAILED_INVALID_MEMORY      "Invalid memory"
#define FAILED_INVALID_ARGUMENT    "Invalid argument"
#define FAILED_INVALID_RESPONSE    "Invalid response"
#define FAILED_INTERNAL_ERROR      "Internal Error"
#define FAILED_CONNECTION_TIMEOUT  "Connection timeout"
#define FAILED_CONNECTION_FAILED   "Connection failed"

#define MEM_PREFIX   "mem://" //内存文件前缀

typedef enum {
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

} CacheAction;

struct FileStat {
	off_t size { 0 };
	time_t mtime { 0 };

	FileStat() {
	}

	FileStat(uint64_t size_, time_t mtim_) :
			size(size_), mtime(mtim_) {
	}
};

#define TIMEOUT 10

class CacheClient;

class CacheFile {
public:
	CacheFile(const std::shared_ptr<CacheClient> & oCacheClient_,
			const std::shared_ptr<BaseSocketClients> & oSocketClients_, const std::string& filename, bool readOnly,
			int timeout_ = 10);

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
	std::shared_ptr<CacheClient> moFileClient;

	std::string msFilename;
	std::string msMessage;

	bool mbReadonly;
	int miTimeout;

	std::shared_ptr<BaseSocketClients> moConnections;
};

class CacheClient: public std::enable_shared_from_this<CacheClient> {
public:
	CacheClient(const std::string & hostinfo_, size_t qsize = 2, int timeout_ = 10);

	~CacheClient();

	/**
	 新建一个文件对象进行读写
	 当 readonly == true  模式，由于使用属性（size、version等）缓存优化 3s，读取效率更高，但当其他块缓存服务器上写入数据的后端存储文件，被当前节点在写入之前已经访问，可能会存在数据不同步问题
	 当 readonly == false 无数据不同步问题
	 或则先 Close 当前块缓存服务器上 filename 的缓存，再访问
	 */
	std::shared_ptr<CacheFile> Open(const std::string & filename, bool readonly = true);

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
	std::shared_ptr<BaseSocketClients> moConnections;
};

inline CacheClient::CacheClient(const std::string& hostinfo_, size_t qsize, int timeout_) :
		msHostinfo(hostinfo_), miTimeout(timeout_) {

	qsize = std::max<size_t>(1, qsize);
	qsize = std::min<size_t>(512, qsize);

	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::CacheClient, " << msHostinfo << ", Pool: " << qsize);

	if (hostinfo_.find("/") == 0) { //unix socket file
		moConnections = std::shared_ptr<BaseSocketClients>(new AsioUnixClients(msHostinfo, qsize + 2));
	} else {
		moConnections = std::shared_ptr<BaseSocketClients>(new AsioTcpClients(msHostinfo, qsize + 2));
	}

	moConnections->start(qsize);
}

inline CacheClient::~CacheClient() {
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::~CacheClient, " << msHostinfo);
}

inline std::shared_ptr<CacheFile> CacheClient::Open(const std::string& filename, bool readonly) {
	auto self = this->shared_from_this();
	return std::make_shared<CacheFile>(self, moConnections, filename, readonly);
}

inline int CacheClient::MkDir(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientMkDir);
	sb->write_str(filename);

	auto conn = moConnections->get_connection();

	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caClientMkDirResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message << ", " << action);

			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
	return tsFailed;
}

inline int CacheClient::RmDir(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientRmDir);
	sb->write_str(filename);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caClientRmDirResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message << ", " << action);

			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
	return tsFailed;
}

inline int CacheClient::Unlink(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientUnlink);
	sb->write_str(filename);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caClientUnlinkResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message << ", " << action);

			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message)
	return tsFailed;
}

inline int CacheClient::GetAttr(const std::string& filename, struct FileStat &stat, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientGetAttr);
	sb->write_str(filename);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::GetAttr: " << message)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caClientGetAttrResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::GetAttr: " << message << ", " << action);

			return tsFailed;
		}

		if (state == tsSuccess) {
			memcpy(&stat, message.c_str(), sizeof(struct FileStat));
			message.clear();
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::GetAttr: " << message)
	return tsFailed;
}

inline int CacheClient::Truncate(const std::string& filename, off_t newsize, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientTruncate);
	sb->write_str(filename);
	sb->write_int64(newsize);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caClientTruncateResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message << ", " << action);

			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message)
	return tsFailed;
}

inline int CacheClient::Close(const std::string& filename, std::string& message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClose);
	sb->write_str(filename);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(message) == false || action != CacheAction::caSuccess) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message << ", " << action);

			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message)
	return tsFailed;
}

inline CacheFile::CacheFile(const std::shared_ptr<CacheClient> & oCacheClient_,
		const std::shared_ptr<BaseSocketClients> & oSocketClients_, const std::string& filename_, bool readonly_,
		int timeout_) :
		moFileClient(oCacheClient_), msFilename(filename_), mbReadonly(readonly_), miTimeout(timeout_), //
		moConnections(oSocketClients_) {

}

#include "read2.inc"

/**
 * 客户端并行分块读取数据流程
 * 	 1、多线程直接从 127.0.0.1:6501 的 块缓存 服务读取各块数据上，在客服端进行数据组装
 * 	 返回 成功读取数据量，<0 代表错误发生
 */
inline ssize_t CacheFile::Read2(void* buffer, size_t buffer_size, off_t offset) {
	if (mbReadonly == false) {
		return Read(buffer, buffer_size, offset);
	}

	std::shared_ptr<AsyncRead2> async_read2 = std::make_shared<AsyncRead2>(moConnections, msFilename, miTimeout);
	return async_read2->Read(buffer, buffer_size, offset, msMessage);
}

/**
 * 客户端串行读取数据流程
 * 	 1、直接从 127.0.0.1:6501 的 块缓存 服务读取数据上，剩余事情由 块缓存 处理
 * 	 返回 成功读取数据量，<0 代表错误发生
 */
inline ssize_t CacheFile::Read(void* buffer, size_t buffer_size, off_t offset) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientRead);
	sb->write_str(msFilename);
	sb->write_int8(mbReadonly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

	sb->write_int64(offset);
	sb->write_uint64(buffer_size);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int32_t state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| action != CacheAction::caClientReadResp) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage << ", " << action)
			return tsFailed;
		}

		if (state != tsSuccess) { //如果 块缓存 上发送错误，则读取错误 message
			if (result->input.read_str(msMessage) == false) {
				msMessage = FAILED_INVALID_RESPONSE;
				LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
				return tsFailed;
			}
			return state;
		}

		size_t bytes_readed = 0;
		if (result->input.read_str((char *) buffer, buffer_size, bytes_readed) == false) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return bytes_readed;
	}

	msMessage = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
	return tsFailed;
}

/**
 * 客户端写入数据流程
 * 	 1、将数据直接写入到 127.0.0.1:6501 的 块缓存 服务上，剩余事情由 块缓存 处理
 * 	 返回 成功写入数据量，<0 代表错误发生
 *
 * 	 注意：如果 两个 client 同时写入数据到相同 块缓存 上，会出现 块缓存 数据混乱，同时底层持久层数据也会混乱
 */
inline ssize_t CacheFile::Write(const void* buffer, size_t buffer_size, off_t offset, bool async_write) {
	if (mbReadonly == true) {
		msMessage = "Readonly File";
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
		return - EROFS;
	}

	if (buffer_size == 0) {
		return 0;
	}

	int8_t write_async = async_write == true ? 1 : 0;

	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientWrite);
	sb->write_str(msFilename);
	sb->write_int8(mbReadonly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

	sb->write_int64(offset);
	sb->write_str((const char *) buffer, buffer_size);
	sb->write_int8(write_async);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int32_t bytes = tsFailed;
		if (result->input.read_int8(action) == false || result->input.read_int32(bytes) == false
				|| action != CacheAction::caClientWriteResp) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage << ", " << action)
			return tsFailed;
		}

		if (result->input.read_str(msMessage) == false) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return bytes;
	}

	msMessage = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
	return tsFailed;
}

inline int CacheFile::Truncate(off_t newsize) {
	if (mbReadonly == true) {
		msMessage = "Readonly File";
		return - EROFS;
	}
	return moFileClient->Truncate(msFilename, newsize, msMessage);
}

inline int CacheFile::Flush() {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientFlush);
	sb->write_str(msFilename);

	auto conn = moConnections->get_connection();
	std::shared_ptr<SyncResult> result = conn->sync_communicate(sb, miTimeout, true);

	if (result->code == SyncResultState::srsTimeout) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage)
		return -EBUSY;
	}

	if (result->code == SyncResultState::srsOK) {
		int8_t action;
		int state = tsFailed;

		if (result->input.read_int8(action) == false || result->input.read_int32(state) == false
				|| result->input.read_str(msMessage) == false || action != CacheAction::caClientFlushResp) {

			if (msMessage.empty() == true) {
				msMessage = FAILED_INVALID_RESPONSE;
			} //

			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage << ", " << action)
			return tsFailed;
		}

		moConnections->reuse_conn(conn);

		return state;
	}

	msMessage = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage)
	return tsFailed;
}

inline int CacheFile::GetAttr(struct FileStat& stat) {
	return moFileClient->GetAttr(msFilename, stat, msMessage);
}

#endif /* CACHECLIENT_HPP_ */
