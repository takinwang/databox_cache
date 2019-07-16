/*
 * backend.hpp
 *
 *  Created on: Oct 13, 2018
 *      Author: root
 */

#ifndef BACKEND_HPP_
#define BACKEND_HPP_

#include <unistd.h>
#include <sys/stat.h>

#include <stdexcept>
#include <string>
#include <memory>

#include <databox/cpl_debug.h>
#include <databox/cpl_conf.hpp>

//#define STATIC_BACKEND_PLUGINS

class BackendFactory;
class BackendManager;

class Backend {
public:
	friend class BackendManager;
protected:
	std::shared_ptr<BackendFactory> oBackendFactory;
public:
	std::string message;
	int code { 0 };

	virtual ~Backend() {
	}

	virtual int Read(void * buffer, size_t size, off_t offset) = 0;

	virtual int Write(void * buffer, size_t size, off_t offset) = 0;

//	virtual int Flush() = 0;

	virtual bool isValid(bool create_if_not_exists) = 0;

	virtual bool GetAttr(const std::string& filename, struct stat * st) = 0;

	virtual bool Truncate(const std::string& filename, off_t length) = 0;
//
	virtual bool Unlink(const std::string& filename) = 0;

	virtual bool MkDir(const std::string& filename) = 0;

	virtual bool RmDir(const std::string& path) = 0;

	const std::shared_ptr<BackendFactory>& getBackendFactory() const {
		return oBackendFactory;
	}

};

class BackendFactory {
private:

	std::string prefix;
	bool bReadOnly { false };

public:
	virtual ~BackendFactory() {
	}

	virtual std::shared_ptr<Backend> Open(const std::string & filename) = 0;

	void Start();

	void Stop();

	std::string Prefix() {
		return prefix;
	}

	/**
	 * 检查文件名是否正确
	 */
	virtual bool Validate(const std::string& path) {
		return true;
	}

	void Load(const ConfReader & cr);

	bool isReadOnly() const {
		return bReadOnly;
	}
};

inline void BackendFactory::Start() {
}

inline void BackendFactory::Stop() {
}

inline void BackendFactory::Load(const ConfReader& cr) {

	prefix = cr.get_string("prefix", "");
	if (prefix.empty() == true) {
		LOGGER_WARN("#" << __LINE__ << ", BackendFactory::Load, Error, No valid Prefix: " << cr.getFilename());
	}

	bReadOnly = cr.get_int("readonly", 0) != 0;
}

#endif /* BACKEND_HPP_ */
