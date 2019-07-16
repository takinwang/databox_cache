/*
 * rados_backend.hpp
 *
 *  Created on: Oct 30, 2018
 *      Author: root
 */

#ifndef BACKEND_RADOS_BACKEND_HPP_
#define BACKEND_RADOS_BACKEND_HPP_

#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include <string>
#include <memory>
#include <atomic>

#include <backend.hpp>

#include <databox/cpl_debug.h>
#include <databox/cpl_conf.hpp>
#include <databox/filesystemutils.hpp>

#include "rados_cc.hpp"

class RadosBackend: public Backend {
private:
	std::shared_ptr<VsiCephClient> oCephClient;
	std::shared_ptr<CephNode> node;
	std::shared_ptr<RadosContext> ctx;

	std::string filename;

	std::string dboxstorage;
	std::string ceph;
	std::string pool;
	std::string dataid;

	std::atomic<int> iOpenFlag { 0 };
protected:
	bool Open(bool create_if_not_exists);

public:
	RadosBackend(const std::shared_ptr<VsiCephClient> & oCephClient, const std::string & filename);

	~RadosBackend();

	int Read(void * buffer, size_t size, off_t offset);

	int Write(void * buffer, size_t size, off_t offset);

	bool Truncate(const std::string& filename, off_t length);

	bool Unlink(const std::string& filename);

	bool MkDir(const std::string& filename);

	bool RmDir(const std::string& filename);

	bool GetAttr(const std::string& filename, struct stat * st);

	bool isValid(bool create_if_not_exists) {
		return Open(create_if_not_exists) == true;
	}
};

class RadosBackendFactory: public BackendFactory {
private:
	std::shared_ptr<VsiCephClient> oCephClient;
	std::string prefix;
public:
	RadosBackendFactory(const std::string & conf);

	std::shared_ptr<Backend> Open(const std::string & filename);

	std::string Prefix() {
		return prefix;
	}

	bool Validate(const std::string& filename);
};

#include "rados_backend.inc"

#endif /* BACKEND_RADOS_BACKEND_HPP_ */
