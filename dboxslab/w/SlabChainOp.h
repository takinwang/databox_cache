class SlabChainOp: public std::enable_shared_from_this<SlabChainOp> {
protected:
	std::shared_ptr<SlabFileManager> oSlabFileManager;
	std::shared_ptr<BackendManager> oBackendManager;
	std::shared_ptr<SlabServerData> oServerdata;

	std::shared_ptr<SlabFile> oSlabFile;
	std::shared_ptr<asio_server_tcp_connection> conn;

	std::string filename;
	off_t offset;
	/**
	 * 用于从后端读取数据的缓存
	 */
	std::shared_array_ptr<char> read_buffer;

	bool bBackendMore { true };

	std::list<uint32_t> oBlockOffsetIds;
	std::map<uint32_t, SlabMeta> oSlabOffsetMetas;

	uint32_t iFisrtBlockOffsetId { 0 }; //开头一块编号
	uint32_t iLastBlockOffsetId { 0 }; //最后一块编号

	std::shared_ptr<boost::asio::io_context::strand> io_strand;
	std::shared_ptr<SlabBlock> oNullSlabBlock;

public:

	typedef std::function<void(int state, const std::string & message, std::shared_ptr<SlabBlock> oSlabBlock)> SlabCallback;

	SlabChainOp(const std::shared_ptr<SlabFileManager> & oSlabFileManager_,
			const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile> & oSlabFile_,
			const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_,
			const std::vector<uint32_t>& oBlockOffsetIds_);

	virtual ~SlabChainOp();

	void PutVersion(uint32_t block_offset_id, int32_t version);

	void PutPeer(uint32_t block_offset_id, const std::shared_ptr<SlabPeer> & oSlabPeer);

	/**
	 * 如果是只读模式，将当前所需块列表的元数据放入到缓存中
	 */
	void PutMetaToCache();

	/*
	 * 从缓存中获取当前所需块列表的元数据，返回 true
	 */
	bool GetMetaFromCache();

protected:

	/**
	 * 删除一块数据缓存邻居信息，这些邻居不可用，直接从底部存储读取数据
	 */
	void RemoveSlabPeer(uint32_t block_offset_id, const std::string & peer);

	/**
	 * 异步方式从各 peer 读取数据，如果没有，调用后台 backend 读取数据
	 * 离线模式下网络故障 不会调用该函数
	 */
	void ReadOneSlabPeer(uint32_t block_offset_id, int32_t mVersion, const SlabCallback & callback, bool offline);

	/**
	 * 从后端存储读取数据
	 */
	void ReadBackend(uint32_t block_offset_id, int32_t mVersion, const SlabCallback & callback, bool offline);

	/**
	 * 保存从邻居或后端存储得到的数据到内存块
	 */
	void LoadDataToSlab(const char * buffer, size_t buffer_size, uint32_t block_offset_id, int32_t mVersion,
			const SlabCallback & callback);
};

/**
 * 局部自引用对象，无多线程使用
 */
class SlabChainReader: public SlabChainOp {
private:

	std::stringstream rb_buffer; //读取数据所需的临时缓存

	size_t bytes_to_read { 0 }; //待读取的长度，对于写入无效
public:
	SlabChainReader(const std::shared_ptr<SlabFileManager> & oSlabFileManager_,
			const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile> & oSlabFile_,
			const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_, size_t read_size_,
			const std::vector<uint32_t>& oBlockOffsetIds_);

	~SlabChainReader();

	/**
	 * 异步读取数据，内部函数调用链
	 */
	void ReadAsync(bool offline);

	void Read(bool offline);

	/**
	 * 读取一块
	 */
	void ReadOneSlab(uint32_t block_offset_id, bool offline);
};

/**
 * 局部自引用对象，无多线程使用
 */
class SlabChainWriter: public SlabChainOp {
private:
	ssize_t bytes_done { 0 }; //成功写入数据数
	bool async_write { true };
	std::string data_to_write; //待写入的数据，对于读取无效

public:
	SlabChainWriter(const std::shared_ptr<SlabFileManager> & oSlabFileManager_,
			const std::shared_ptr<SlabServerData>& serverdata_, const std::shared_ptr<SlabFile> & oSlabFile_,
			const std::shared_ptr<asio_server_tcp_connection>& conn_, off_t offset_, const std::string & write_data_,
			bool async_write_, const std::vector<uint32_t>& oBlockOffsetIds_);

	~SlabChainWriter();

	void WriteAsync(bool offline);

	void Write(bool offline);

	/**
	 *  修改一块
	 */
	void WriteOneSlab(uint32_t block_offset_id, bool offline);

	/**
	 * 分配一个新内存，写入数据
	 */
	void SaveDataToNewSlab(uint32_t block_offset_id, int32_t mVersion, bool offline);

	/**
	 * 寻找内存块，更新数据到内存块
	 */
	bool SaveDataToOldSlab(uint32_t block_offset_id, int32_t mVersion, bool offline);

	/**
	 * 确认修改一块成功
	 */
	void WriteSlabMeta(uint32_t block_offset_id, const std::shared_ptr<SlabBlock> & oSlabBlock, int slab_length,
			bool offline);

	bool SaveBackend(size_t block_offset_id, const std::shared_ptr<SlabBlock> & oSlabBlock, int & code,
			std::string & message);
};
