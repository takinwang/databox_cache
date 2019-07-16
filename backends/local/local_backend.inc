
inline LocalBackendFactory::LocalBackendFactory(const std::string & conf) {
	ConfReader cr;
	cr.load(conf);
	root = cr.get_string("root", "/tmp");

	this->Load(cr);
}

/**
 * local://abc.txt
 */
inline std::shared_ptr<Backend> LocalBackendFactory::Open(const std::string & filename) {
	return std::shared_ptr < Backend > (new LocalBackend(root, filename));
}

inline LocalBackend::LocalBackend(const std::string & root_, const std::string & filename_) :
		root(root_), filename(filename_) {
	filename = Normalize(filename);
}

inline LocalBackend::~LocalBackend() {
	if (fd >= 0) {
		close (fd);
	}
	fd = -1;
}

inline int LocalBackend::Read(void* buffer, size_t size, off_t offset) {
	code = 0;
	if (Open(false) == false) {
		return -1;
	}

	int bytes = pread(fd, buffer, size, offset);
	if (bytes < 0) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::Read: " << filename << ", Error: " << message);
	}
	return bytes;
}

inline int LocalBackend::Write(void* buffer, size_t size, off_t offset) {
	code = 0;
	if (Open(true) == false) {
		return -1;
	}

	int bytes = pwrite(fd, buffer, size, offset);
	if (bytes < 0) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::Write: " << filename << ", Error: " << message);
	}
	return bytes;
}

inline bool LocalBackend::Unlink(const std::string& filename) {
	code = 0;
	std::string path = Normalize(filename);
	if (Validate(path) == false) {
		return false;
	}

	if (unlink(path.c_str()) == -1) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::Unlink: " << path << ", Error: " << message);
		return false;
	}
	return true;
}

inline bool LocalBackend::MkDir(const std::string& filename) {
	code = 0;
	std::string path = Normalize(filename);
	if (Validate(path) == false) {
		return false;
	}

	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
	if (mkdir(path.c_str(), mode) == -1) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::MkDir: " << path << ", Error: " << message);
		return false;
	}
	return true;
}

inline bool LocalBackend::RmDir(const std::string& filename) {
	code = 0;
	std::string path = Normalize(filename);
	if (Validate(path) == false) {
		return false;
	}

	if (rmdir(path.c_str()) == -1) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::RmDir: " << path << ", Error: " << message);
		return false;
	}
	return true;
}

inline bool LocalBackend::Truncate(const std::string& filename, off_t length) {
	code = 0;
	std::string path = Normalize(filename);
	if (Validate(path) == false) {
		return false;
	}

	if (truncate(path.c_str(), length) == -1) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::Truncate: " << path << ", Error: " << message);
		return false;
	}
	return true;
}

inline bool LocalBackend::GetAttr(const std::string& filename, struct stat* st) {
	code = 0;
	std::string path = Normalize(filename);
	if (Validate(path) == false) {
		return false;
	}

	if (stat(path.c_str(), st) == -1) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::GetAttr: " << path << ", Error: " << message);
		return false;
	}
	return true;
}

inline bool LocalBackend::Open(bool create_if_not_exists) {
	code = 0;

	int flag = O_RDWR;
	if (create_if_not_exists == true) {
		if (iOpenWrite.fetch_add(1) == 0) {
			iOpenFlag.store(0);
		}
		flag = O_RDWR | O_CREAT;
	}

	if (iOpenFlag.fetch_add(1) > 0) {
		return fd >= 0;
	}

	if (fd >= 0) {
		close (fd);
		fd = -1;
	}

	if (Validate(filename) == false) {
		return false;
	}

	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
	fd = open(filename.c_str(), flag, mode);
	if (fd < 0) {
		code = errno;
		message = strerror(code);
		LOGGER_WARN("#" << __LINE__ << ", LocalBackend::Open: " << filename << ", Error: " << message);
		return false;
	}

	return true;
}

inline std::string LocalBackend::Normalize(const std::string& rel_path) {
	code = 0;
	return FileSystemUtils::NormalizePath(FileSystemUtils::JoinPath(root, rel_path));
}

inline bool LocalBackend::Validate(const std::string& abs_path) {
	code = 0;
	if (abs_path.find("..") != abs_path.npos || abs_path.find("~") != abs_path.npos) {
		code = EINVAL;
		message = "Invalid argument";
		return false;
	}
	ssize_t pos = abs_path.find(root);
	if (pos == 0) {
		return true;
	}
	code = EINVAL;
	message = "Invalid argument";
	return false;
}
