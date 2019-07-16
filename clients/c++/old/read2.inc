class AsyncRead2: public std::enable_shared_from_this<AsyncRead2> {
private:

	std::string msFilename;
	int miTimeout;

	std::shared_ptr<BaseSocketClients> moConnections;

protected:
	int CalcOffsetBlocks(off_t offset, size_t size, std::vector<int32_t> & BlockOffsetIds);

public:

	AsyncRead2(const std::shared_ptr<BaseSocketClients> & oCacheClient_, const std::string& filename_, int timeout_) :
			msFilename(filename_), miTimeout(timeout_), moConnections(oCacheClient_) {

	}

	ssize_t Read(void* buffer, size_t buffer_size, off_t offset, std::string & msMessage);

};

struct AsyncData {
	int offset_id;
	std::shared_ptr<SyncResult> result;
};

class AsyncReaderHelper {
private:
	std::atomic_int count_dec { 0 };
	std::atomic_int count_inc { 0 };

	std::mutex mtx;
	std::list<std::shared_ptr<AsyncData>> results;
public:
	AsyncReaderHelper(int n) :
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

	void push(int offset_id, const std::shared_ptr<SyncResult> & result_) {
		std::lock_guard < std::mutex > l(mtx);

		std::shared_ptr<AsyncData> result = std::make_shared<AsyncData>();

		result->offset_id = offset_id;
		result->result = result_;

		results.push_back(result);
	}

	bool pop(std::shared_ptr<AsyncData> & result) {
		std::lock_guard < std::mutex > l(mtx);
		if (results.size() == 0) {
			return false;
		}

		result = results.front();
		results.pop_front();

		return true;
	}

	void clear() {
		std::lock_guard < std::mutex > l(mtx);
		results.clear();
	}

public:
	std::promise<int> moPromise;
	std::string msMessage;
};

inline int AsyncRead2::CalcOffsetBlocks(off_t offset, size_t size, std::vector<int32_t>& BlockOffsetIds) {
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

inline ssize_t AsyncRead2::Read(void* buffer, size_t buffer_size, off_t offset, std::string & msMessage) {

	std::vector < int32_t > BlockOffsetIds;
	if (CalcOffsetBlocks(offset, buffer_size, BlockOffsetIds) == 0) {
		return 0;
	}

	LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read: " << msFilename << ", Blocks: " << BlockOffsetIds.size());

	bool mbReadonly = true;
	auto self = shared_from_this();

	int nBlocks = BlockOffsetIds.size();
	std::shared_ptr<AsyncReaderHelper> oHelper = std::make_shared < AsyncReaderHelper > (nBlocks);

	for (int32_t offset_id : BlockOffsetIds) {
		std::shared_ptr < AsioTaskData > a_task = std::make_shared<AsioTaskData>();

		a_task->retry_if_error = false;
//		a_task->

		a_task->output->write_int8(CacheAction::caClientRead2);
		a_task->output->write_str(msFilename);
		a_task->output->write_int8(mbReadonly == true ? 1 : 0); //是否只读，如果只读，数据块端将进行通讯优化

		a_task->output->write_int32(offset_id);

		a_task->callback =
				[this, self, oHelper, offset_id ] ( const std::shared_ptr<SyncResult> & result ) {

					if( result->code != SyncResultState::srsOK ) {
						/**
						 * 错误了
						 */
						if( oHelper->inc() == 1 ) {
							/**
							 * 保存第一个错误信息
							 */
							oHelper->msMessage = result->message;
						}
					}

					if ( oHelper->inc_val() > 0 ) {
						LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, Some Errors: " << msFilename << ", " << oHelper->inc_val() );
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
					oHelper->push( offset_id, result );

					if( oHelper->dec() == 0 ) {
						LOGGER_TRACE( "#" << __LINE__ << ", AsyncRead2::Read: " << msFilename << ", " << offset_id);

						/**
						 * 全部收到数据后，退出 waitfor
						 */
						oHelper->moPromise.set_value( 1 );
						return;
					}
				};

		moConnections->Post(a_task);
	}

	std::future<int> future = oHelper->moPromise.get_future();

	std::future_status status = future.wait_for(std::chrono::seconds(miTimeout));

	if (status != std::future_status::ready) {
		LOGGER_TRACE("#" << __LINE__ << ", AsyncRead2::Read, timeout: " << msFilename);

		msMessage = "timeout";
		return tsTimeout;
	}

	if (future.get() < 0) {
		msMessage = oHelper->msMessage;
		return -1;
	}

	size_t bytes_readed = 0;

	int offset_id_0 = offset / SIZEOFBLOCK;
	off_t offset_0 = offset - (offset_id_0 * SIZEOFBLOCK);
	size_t block_size_0 = SIZEOFBLOCK - offset_0;

	for (;;) {
		std::shared_ptr<AsyncData> oAsyncData;
		if (oHelper->pop(oAsyncData) == false) {
			break;
		}

		int8_t action;
		int32_t state = tsFailed;

		if (oAsyncData->result->input.read_int8(action) == false || oAsyncData->result->input.read_int32(state) == false
				|| action != CacheAction::caClientReadResp) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read2, " << msMessage << ", " << action)
			return tsFailed;
		}

		if (state != tsSuccess) { //如果 块缓存 上发送错误，则读取错误 message
			if (oAsyncData->result->input.read_str(msMessage) == false) {
				msMessage = FAILED_INVALID_RESPONSE;
				LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read2, " << msMessage)
				return tsFailed;
			}
			return state;
		}

		std::string data;
		if (oAsyncData->result->input.read_str(data) == false) {
			msMessage = FAILED_INVALID_RESPONSE;
			LOGGER_TRACE("#" << __LINE__ << ", CacheFile::Read2: " << msMessage)
			return tsFailed;
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
