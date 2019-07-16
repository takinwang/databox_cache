inline RadosBackendFactory::RadosBackendFactory(const std::string& conf) {
	oCephClient = std::make_shared < VsiCephClient > (conf);

	ConfReader cr;
	cr.load(conf);

	this->Load(cr);
}

inline bool RadosBackendFactory::Validate(const std::string& filename) {
	std::vector < std::string > items;
	StringUtils::Split(filename, items, PathSep);

	if (items.size() < 3) {
		return false;
	}

	const std::string & ceph = items[0];
	const std::shared_ptr<CephNode> & node = oCephClient->find(ceph);

	if (node.get() == NULL) {
		return false;
	}

	return true;
}

/**
 * rados://<cluster>/<pool>/<abc/abc.txt>
 */
inline std::shared_ptr<Backend> RadosBackendFactory::Open(const std::string& filename) {
	return std::shared_ptr < Backend > (new RadosBackend(oCephClient, filename));
}

inline RadosBackend::RadosBackend(const std::shared_ptr<VsiCephClient>& oCephClient, const std::string& filename) {
	this->oCephClient = oCephClient;
	this->filename = FileSystemUtils::NormalizePath(filename);
}

inline RadosBackend::~RadosBackend() {
}

inline bool RadosBackend::Open(bool create_if_not_exists) {
	code = 0;
	if (iOpenFlag.fetch_add(1) > 0) {
		if (ctx != NULL && ctx->ioctx() != NULL) {
			return true;
		}
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Open, " << filename);

	std::vector < std::string > items;
	StringUtils::Split(filename, items, PathSep);

	if (items.size() < 3) {
		errno = code = EINVAL;
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Open, " << filename << ", Error: " << message);
		return false;
	}

	ceph = items[0];
	pool = items[1];
	dataid = StringUtils::Join(items, PathSep, 2);

	std::shared_ptr < CephNode > node;
	if (oCephClient->connect(ceph, node) < 0) {
		code = errno;
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Open, connect, " << filename << ", Error: " << message);
		return false;
	}

	ctx = node->ctx(pool);

	if (ctx->ioctx() == NULL) {
		code = errno;
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Open, ioctx, " << filename << ", Error: " << message);
		return false;
	}

	return true;
}

inline int RadosBackend::Read(void* buffer, size_t size, off_t offset) {
	code = 0;
	if (Open(true) == false) {
		return -1;
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", RadosBackend::Read, " << pool << "|" << dataid << ", size: " << size << ", offset: "
					<< offset);

	int bytes = rados_read(ctx->ioctx(), dataid.c_str(), (char *) buffer, size, offset);
	if (bytes < 0) {
		errno = code = abs(bytes);
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Read, " << filename << ", Error: " << message);
	}
	return bytes;
}

inline int RadosBackend::Write(void* buffer, size_t size, off_t offset) {
	code = 0;
	if (Open(true) == false) {
		return -1;
	}

	LOGGER_TRACE(
			"#" << __LINE__ << ", RadosBackend::Write, " << pool << "|" << dataid << ", size: " << size << ", offset: "
					<< offset);

	int bytes = rados_write(ctx->ioctx(), dataid.c_str(), (const char *) buffer, size, offset);
	if (bytes == 0) {
		bytes = size;
	}

	if (bytes < 0) {
		errno = code = abs(bytes);
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Write, " << filename << ", Error: " << message);
	}

	return bytes;
}

inline bool RadosBackend::Truncate(const std::string& filename, off_t length) {
	code = 0;
	if (Open(true) == false) {
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Truncate, " << pool << "|" << dataid << ", size: " << length);

	int state = rados_trunc(ctx->ioctx(), dataid.c_str(), length);

	if (state < 0) {
		errno = code = abs(state);
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Write, " << filename << ", Error: " << message);
		return false;
	}

	return true;
}

inline bool RadosBackend::MkDir(const std::string& filename) {
	code = 0;
	return true;
}

inline bool RadosBackend::RmDir(const std::string& filename) {
	code = 0;
	return true;
}

inline bool RadosBackend::Unlink(const std::string& filename) {
	code = 0;
	if (Open(true) == false) {
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Unlink, " << pool << "|" << dataid);
	int state = rados_remove(ctx->ioctx(), dataid.c_str());

	if (state < 0) {
		errno = code = abs(state);
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::Unlink, " << filename << ", Error: " << message);
		return false;
	}

	return true;
}

inline bool RadosBackend::GetAttr(const std::string& filename, struct stat* st) {
	code = 0;
	if (Open(true) == false) {
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::GetAttr, " << pool << "|" << dataid);
	bzero(st, sizeof(struct stat));

	uint64_t nLength { 0 };
	time_t nMTime { 0 };

	int state = rados_stat(ctx->ioctx(), dataid.c_str(), &nLength, &nMTime);

	if (state < 0) {
		errno = code = abs(state);
		message = strerror(code);
		LOGGER_TRACE("#" << __LINE__ << ", RadosBackend::GetAttr, " << filename << ", Error: " << message);
		return false;
	}

	st->st_mtim.tv_sec = nMTime;
	st->st_size = nLength;

	return true;
}
