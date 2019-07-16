#include "CacheClient.hpp"

namespace DVB {

CachedFiles DboxSlabClient::oCachedFiles;

/**
 * 初始化内部工作线程，不调用将不可用
 */
void DboxSlabClient::initialize(uint32_t qsize) {
	const auto & ios = AsyncIOService::getInstance();
	ios->start(qsize, false);
}

/**
 * 退出内部工作线程，调用后将不可用
 */
void DboxSlabClient::finalize() {
	const auto & ios = AsyncIOService::getInstance();
	ios->stop();
	ios->join();
}

DboxSlabClient::DboxSlabClient(const std::string& hostinfo_, int timeout_) :
		msHostinfo(hostinfo_), miTimeout(timeout_) {

	moAsyncIOService = AsyncIOService::getInstance();

	if (moAsyncIOService->started() == false) {
		moAsyncIOService->start(4, false);
	}

	const auto & ios = moAsyncIOService->getIoService();

	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::CacheClient, " << msHostinfo);

	if (hostinfo_.find("/") == 0) { //unix socket file
		moMsgSender = std::shared_ptr<BaseMsgSender>(new UnixMsgSender(ios, msHostinfo));
	} else {
		std::string host, port;
		ParseHostInfo(hostinfo_, host, port);
		moMsgSender = std::shared_ptr<BaseMsgSender>(new TcpMsgSender(ios, host, port));
	}
}

DboxSlabClient::~DboxSlabClient() {
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::~CacheClient, " << msHostinfo);
}

std::shared_ptr<VFile> DboxSlabClient::Open(const std::string& filename, const std::string & mode) {
	auto self = this->shared_from_this();
	return std::make_shared<VFile>(self, moMsgSender, filename, mode);
}

int DboxSlabClient::MkDir(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientMkDir);
	sb->write_str(filename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caClientMkDirResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message << ", " << action);

			return tsFailed;
		}

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
	return tsFailed;
}

int DboxSlabClient::RmDir(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientRmDir);
	sb->write_str(filename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caClientRmDirResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message << ", " << action);

			return tsFailed;
		}

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::MkDir: " << message);
	return tsFailed;
}

int DboxSlabClient::Unlink(const std::string& filename, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientUnlink);
	sb->write_str(filename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caClientUnlinkResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message << ", " << action);

			return tsFailed;
		}
		oCachedFiles.erase(filename);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Unlink: " << message)
	return tsFailed;
}

int DboxSlabClient::GetAttr(const std::string& filename, struct FileStat &stat, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientGetAttr);
	sb->write_str(filename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::GetAttr: " << message)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caClientGetAttrResp) {

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

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::GetAttr: " << message)
	return tsFailed;
}

int DboxSlabClient::Truncate(const std::string& filename, off_t newsize, std::string & message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientTruncate);
	sb->write_str(filename);
	sb->write_int64(newsize);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caClientTruncateResp) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message << ", " << action);

			return tsFailed;
		}

		oCachedFiles.erase(filename);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Truncate: " << message)
	return tsFailed;
}

int DboxSlabClient::Close(const std::string& filename, std::string& message) {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClose);
	sb->write_str(filename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		message = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(message) == false || action != CacheAction::caSuccess) {

			if (message.empty() == true) {
				message = FAILED_INVALID_RESPONSE;
			}

			LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message << ", " << action);

			return tsFailed;
		}

		oCachedFiles.erase(filename);

		return state;
	}

	message = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheClient::Close: " << message)
	return tsFailed;
}

struct AsyncData {
	int offset_id;
	std::shared_ptr<stringbuffer> result;
	std::shared_ptr<CachedFiles::CachedData> cache;
};

class AsyncHelper {
private:
	std::atomic_int count_dec { 0 };
	std::atomic_int count_inc { 0 };

	std::mutex mtx;
	std::list<std::shared_ptr<AsyncData>> results;
public:
	AsyncHelper(int n) :
			count_dec(n) {
	}

	int dec() {
		return --count_dec;
	}

	int dec_val() {
		return count_dec;
	}

	int inc() {
		return ++count_inc;
	}

	int inc_val() {
		return count_inc;
	}

	void push(int offset_id, const std::shared_ptr<stringbuffer> & result_) {
		std::lock_guard<std::mutex> l(mtx);

		std::shared_ptr<AsyncData> result = std::make_shared<AsyncData>();

		result->offset_id = offset_id;
		result->result = result_;

		results.push_back(result);
	}

	void push(int offset_id, const std::shared_ptr<CachedFiles::CachedData> & result_) {
		std::lock_guard<std::mutex> l(mtx);

		std::shared_ptr<AsyncData> result = std::make_shared<AsyncData>();

		result->offset_id = offset_id;
		result->cache = result_;

		results.push_back(result);
	}

	bool pop(std::shared_ptr<AsyncData> & result) {
		std::lock_guard<std::mutex> l(mtx);
		if (results.size() == 0) {
			return false;
		}

		result = results.front();
		results.pop_front();

		return true;
	}

	void clear() {
		std::lock_guard<std::mutex> l(mtx);
		results.clear();
	}

public:
	std::promise<int> moPromise;
	std::string msMessage;
};

class AsyncRead2: public std::enable_shared_from_this<AsyncRead2> {
private:
	std::string msFilename;
	int miTimeout;

	std::shared_ptr<BaseMsgSender> moMsgSender;

protected:
	int CalcOffsetBlocks(off_t offset, size_t size, std::vector<int32_t> & BlockOffsetIds) {
		if (size <= 0) {
			return 0;
		}

		if (offset < 0) {
			offset = 0;
		}

		off_t uend = offset + size;
		off_t ubeg = (offset / SIZEOFBLOCK) * SIZEOFBLOCK;

		while (ubeg < uend) {
			int32_t block_id = ubeg / SIZEOFBLOCK;
			BlockOffsetIds.push_back(block_id);
			ubeg += SIZEOFBLOCK;
		}

		return BlockOffsetIds.size();
	}

public:

	AsyncRead2(const std::shared_ptr<BaseMsgSender> & moMsgSender_, const std::string& filename_, int timeout_) :
			msFilename(filename_), miTimeout(timeout_), moMsgSender(moMsgSender_) {

	}

	ssize_t Read(void* buffer, size_t buffer_size, off_t offset, std::string & msMessage) {

		std::vector<int32_t> BlockOffsetIds;
		if (CalcOffsetBlocks(offset, buffer_size, BlockOffsetIds) == 0) {
			return 0;
		}

		LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read: " << msFilename << ", Chunks: " << BlockOffsetIds.size());

		bool mbReadonly = true;
		auto self = shared_from_this();

		int nBlocks = BlockOffsetIds.size();
		std::shared_ptr<AsyncHelper> oHelper = std::make_shared<AsyncHelper>(nBlocks);

		int toAsync = 0;

		for (int32_t offset_id : BlockOffsetIds) {
			std::shared_ptr<CachedFiles::CachedData> data;
			if (DboxSlabClient::oCachedFiles.get(msFilename, offset_id, data) == true && data.get() != NULL) {
				/**
				 * 如果缓存中有，直接使用
				 */

				LOGGER_TRACE(
						"#" << __LINE__ << ", AsyncRead2::Read: " << msFilename << ", Hit cache, chunk_id: " << offset_id);
				oHelper->dec();
				oHelper->push(offset_id, data);
				continue;
			}

			/**
			 * 从远程拿数据回来
			 */
			toAsync++;
			std::shared_ptr<TcpMessage> message = std::make_shared<TcpMessage>();

			message->output->write_int8(CacheAction::caClientRead2);
			message->output->write_str(msFilename);
			message->output->write_int8(mbReadonly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

			message->output->write_int32(offset_id);

			message->callback =
					[this, self, oHelper, offset_id ] ( std::shared_ptr<stringbuffer> input, const boost::system::error_code & ec,
							std::shared_ptr<base_connection> conn ) {

						if( ec ) {
							/**
							 * 错误了
							 */
							if( oHelper->inc() == 1 ) {
								/**
								 * 保存第一个错误信息
								 */
								oHelper->msMessage = ec.message();
								/**
								 * 清除其他连接
								 */
								moMsgSender->clear();
							}
						}

						if ( oHelper->inc_val() > 0 ) {
							LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, Some chunks error: " << msFilename << ", " << oHelper->inc_val() );
							/**
							 * 说明其他块有问题，不能使用了，退出 waitfor
							 */
							oHelper->moPromise.set_value( -1 );
							oHelper->clear();
							return;
						}

						/**
						 * 保存结果
						 */
						oHelper->push( offset_id, input );

						if( oHelper->dec() == 0 ) {
							/**
							 * 全部收到数据后，退出 waitfor
							 */
							oHelper->moPromise.set_value( 1 );
							return;
						}
					};

			moMsgSender->PostMessage(message);
		}

		if (toAsync > 0) {
			/**
			 * 部分数据不在本地缓存，需要同步等待数据回来
			 */
			LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, Wait chunks ready: " << msFilename);

			std::future<int> future = oHelper->moPromise.get_future();

			std::future_status status = future.wait_for(std::chrono::seconds(miTimeout));

			if (status != std::future_status::ready) {
				LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, Wait chunks timeout: " << msFilename);

				msMessage = "timeout";
				return tsTimeout;
			}

			if (future.get() < 0) {
				msMessage = oHelper->msMessage;
				return -EIO;
			}
		}

		LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, All chunks done: " << msFilename);

		size_t bytes_readed = 0;

		int offset_id_0 = offset / SIZEOFBLOCK;
		off_t offset_0 = offset - (offset_id_0 * SIZEOFBLOCK);
		size_t block_size_0 = SIZEOFBLOCK - offset_0;

		for (;;) {
			std::shared_ptr<AsyncData> oAsyncData;
			if (oHelper->pop(oAsyncData) == false) {
				break;
			}

			std::string data;
			if (oAsyncData->result.get() != NULL) {
				/**
				 * 当前数据是从远程拿过来的
				 */
				int8_t action;
				int32_t state = tsFailed;

				if (oAsyncData->result->read_int8(action) == false || oAsyncData->result->read_int32(state) == false
						|| action != CacheAction::caClientReadResp) {
					msMessage = FAILED_INVALID_RESPONSE;
					LOGGER_TRACE(
							"#" << __LINE__ << ", CacheFile::Read2, Assemble chunks error: " << msMessage << ", " << oAsyncData->offset_id)
					return tsFailed;
				}

				if (state != tsSuccess) { //如果 块缓存 上发送错误，则读取错误 message
					if (oAsyncData->result->read_str(msMessage) == false) {
						msMessage = FAILED_INVALID_RESPONSE;
						LOGGER_TRACE(
								"#" << __LINE__ << ", CacheFile::Read2, , Assemble chunks error: " << msMessage<< ", " << oAsyncData->offset_id)
						return tsFailed;
					}
					return -abs(state);
				}

				if (oAsyncData->result->read_str(data) == false) {
					msMessage = FAILED_INVALID_RESPONSE;
					LOGGER_TRACE(
							"#" << __LINE__ << ", CacheFile::Read2: , Assemble chunks error: " << msMessage<< ", " << oAsyncData->offset_id)
					return tsFailed;
				}

				/**
				 * 从远程拿来的数据，放入缓存中
				 */

				DboxSlabClient::oCachedFiles.put(msFilename, oAsyncData->offset_id, data);
			}

			if (oAsyncData->cache.get() != NULL) {
				/**
				 * 当前数据是来自缓存，直接使用
				 */
				data = oAsyncData->cache->value;
				LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read2: , Use cached chunk, " << oAsyncData->offset_id)
			}

			size_t src_data_size = data.size();
			if (src_data_size == 0) {
				continue;
			}

			/**
			 * 需要计算每块在 buffer 中的偏移，以及每块的数据起点
			 */

			/**
			 * 该块对应于文件中的 offset
			 */
			off_t block_offset_t = oAsyncData->offset_id * SIZEOFBLOCK;

			/**
			 * 块内偏移
			 */
			int src_offset_0 = 0;
			if (offset > block_offset_t) {
				/**
				 * 第一块中需要跳过的数据
				 */
				src_offset_0 = offset - block_offset_t;
			}

			/**
			 * 每个块内实际数据需要读取的数据量
			 */
			int src_buffer_size = std::min<int>(src_data_size, SIZEOFBLOCK) - src_offset_0;

			/**
			 * 每个块内数据实际读取起始偏移位置
			 */
			const char * src_buffer = data.c_str();
			const char * src_buffer_ptr = src_buffer + src_offset_0;

			/**
			 * 每个块在输出数据缓存区的偏移位置
			 */
			char * dst_buffer = static_cast<char *>(buffer);
			char * dst_buffer_ptr = dst_buffer;

			if (oAsyncData->offset_id > offset_id_0) {
				size_t offset_n = (oAsyncData->offset_id - offset_id_0 - 1) * SIZEOFBLOCK + block_size_0;
				dst_buffer_ptr = dst_buffer + offset_n;
			}

			int dst_buffer_size = SIZEOFBLOCK;
			if (oAsyncData->offset_id == offset_id_0) {
				dst_buffer_size = block_size_0;
			}

			off_t offset_1 = offset + buffer_size;
			off_t block_offset_1 = (oAsyncData->offset_id + 1) * SIZEOFBLOCK;
			if (offset_1 < block_offset_1) {
				dst_buffer_size -= (block_offset_1 - offset_1);
			}

			/////

			int new_buffer_size = std::min<int>(dst_buffer_size, src_buffer_size);
			memcpy(dst_buffer_ptr, src_buffer_ptr, new_buffer_size);

			bytes_readed += new_buffer_size;
		}

		return bytes_readed;
	}

};

VFile::VFile(const std::shared_ptr<DboxSlabClient> & oCacheClient_, const std::shared_ptr<BaseMsgSender>& oMsgSender_,
		const std::string& filename_, const std::string & mode_, int timeout_) :
		moFileClient(oCacheClient_), moMsgSender(oMsgSender_), msFilename(filename_),  //
		msMode(mode_), miTimeout(timeout_) {

	if (mode_.find("w") == mode_.npos) {
		/**
		 * 只读模式
		 */
		bReadOnly = true;
	}

//	if (mode_.find("a") != mode_.npos) {
//		/**
//		 * 追加读写
//		 */
//		bAppend = true;
//	}
}

/**
 * 客户端并行分块读取数据流程
 * 	 1、多线程直接从 127.0.0.1:6501 的 块缓存 服务读取各块数据上，在客服端进行数据组装
 * 	 返回 成功读取数据量，<0 代表错误发生
 */
ssize_t VFile::Read2(void* buffer, size_t buffer_size, off_t offset) {
	if (bReadOnly == false || buffer_size < SIZEOFBLOCK) {
		return Read(buffer, buffer_size, offset);
	}

	TRACE_TIMER(t, "Read2")
	std::shared_ptr<AsyncRead2> async_read2 = std::make_shared<AsyncRead2>(moMsgSender, msFilename, miTimeout);

	ssize_t bytes_readed = async_read2->Read(buffer, buffer_size, offset, msMessage);
	if (bytes_readed >= 0) {
		return bytes_readed;
	}

	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read2: " << msMessage)
	return bytes_readed;
}

/**
 * 客户端串行读取数据流程
 * 	 1、直接从 127.0.0.1:6501 的 块缓存 服务读取数据上，剩余事情由 块缓存 处理
 * 	 返回 成功读取数据量，<0 代表错误发生
 */
ssize_t VFile::Read(void* buffer, size_t buffer_size, off_t offset) {
	TRACE_TIMER(t, "Read1")

	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientRead);
	sb->write_str(msFilename);
	sb->write_int8(bReadOnly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

	sb->write_int64(offset);
	sb->write_uint64(buffer_size);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int32_t state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| action != CacheAction::caClientReadResp) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage << ", " << action)
			return tsFailed;
		}

		if (state != tsSuccess) { //如果 块缓存 上发送错误，则读取错误 message
			if (result->input->read_str(msMessage) == false) {
				msMessage = FAILED_INVALID_RESPONSE;
				LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
				return tsFailed;
			}
			return state;
		}

		size_t bytes_readed = 0;
		if (result->input->read_str((char *) buffer, buffer_size, bytes_readed) == false) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read: " << msMessage)
			return tsFailed;
		}

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
ssize_t VFile::Write(const void* buffer, size_t buffer_size, off_t offset, bool async_write) {
	if (bReadOnly == true) {
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
	sb->write_int8(bReadOnly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

	sb->write_int64(offset);
	sb->write_str((const char *) buffer, buffer_size);
	sb->write_int8(write_async);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int32_t bytes = tsFailed;
		if (result->input->read_int8(action) == false || result->input->read_int32(bytes) == false
				|| action != CacheAction::caClientWriteResp) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage << ", " << action)
			return tsFailed;
		}

		if (result->input->read_str(msMessage) == false) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
			return tsFailed;
		}

		return bytes;
	}

	msMessage = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Write: " << msMessage)
	return tsFailed;
}

int VFile::Truncate(off_t newsize) {
	if (bReadOnly == true) {
		msMessage = "Readonly File";
		return - EROFS;
	}

	return moFileClient->Truncate(msFilename, newsize, msMessage);
}

int VFile::Flush() {
	std::shared_ptr<stringbuffer> sb = std::make_shared<stringbuffer>();

	sb->write_int8(CacheAction::caClientFlush);
	sb->write_str(msFilename);

	const auto & result = moMsgSender->SendMessage(sb, miTimeout);

	if (result->timeout() == true) {
		msMessage = FAILED_CONNECTION_TIMEOUT;
		LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage)
		return -EBUSY;
	}

	if (result->ok() == true) {
		int8_t action;
		int state = tsFailed;

		if (result->input->read_int8(action) == false || result->input->read_int32(state) == false
				|| result->input->read_str(msMessage) == false || action != CacheAction::caClientFlushResp) {

			if (msMessage.empty() == true) {
				msMessage = FAILED_INVALID_RESPONSE;
			} //

			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage << ", " << action)
			return tsFailed;
		}

		return state;
	}

	msMessage = FAILED_CONNECTION_FAILED;
	LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Flush: " << msMessage)
	return tsFailed;
}

int VFile::GetAttr(struct FileStat& stat) {
	return moFileClient->GetAttr(msFilename, stat, msMessage);
}

void CachedFiles::put(const std::string& file, int32_t block_id, const std::string& value) {
	std::shared_ptr<CachedChunks> oCachedChunks;
	oCachedFiles.get_or_create(file, oCachedChunks, 0, []() {
		return std::make_shared<CachedChunks>();
	});

	if (oCachedChunks.get() == NULL) {
		return;
	}

	const std::shared_ptr<CachedData> & data = std::make_shared<CachedData>();
	data->value = value;

	oCachedChunks->Chunks.put(block_id, data, 0);
}

bool CachedFiles::get(const std::string& file, int32_t block_id, std::shared_ptr<CachedData> & data) {
	std::shared_ptr<CachedChunks> oCachedChunks;
	if (oCachedFiles.get(file, oCachedChunks) == false || oCachedChunks.get() == NULL) {
		return false;
	}
	return oCachedChunks->Chunks.get(block_id, data, false);
}

void CachedFiles::erase(const std::string& file) {
	oCachedFiles.erase(file);
}

void CachedFiles::erase(const std::string& file, int32_t block_id) {
	std::shared_ptr<CachedChunks> oCachedChunks;
	if (oCachedFiles.get(file, oCachedChunks) == false || oCachedChunks.get() == NULL) {
		return;
	}
	oCachedChunks->Chunks.erase(block_id);
}

}
;
