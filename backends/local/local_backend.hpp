/*
 * local_backend.hpp
 *
 *  Created on: Oct 13, 2018
 *      Author: root
 */

#ifndef LOCAL_BACKEND_HPP_
#define LOCAL_BACKEND_HPP_

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

class LocalBackend: public Backend {
private:
	int fd { -1 };

	std::atomic<int> iOpenWrite { 0 };
	std::atomic<int> iOpenFlag { 0 };

	std::string root;
	std::string filename;

protected:
	bool Open(bool create_if_not_exists);

	std::string Normalize(const std::string & rel_path);

	bool Validate(const std::string & abs_path);

public:
	LocalBackend(const std::string & root, const std::string & filename);

	~LocalBackend();

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

class LocalBackendFactory: public BackendFactory {
private:
	std::string root { "/tmp" };
public:
	LocalBackendFactory(const std::string & conf);

	std::shared_ptr<Backend> Open(const std::string & filename);

};

#include "local_backend.inc"

#endif /* LOCAL_BACKEND_HPP_ */
