/*
 * dboxslab_swig.hpp
 *
 *  Created on: Apr 20, 2018
 *      Author: root
 */

#ifndef DBOXSLAB_SWIG_HPP_
#define DBOXSLAB_SWIG_HPP_

#include <python3.6m/Python.h>

#include <stdexcept>

#include "../c++/CacheClient.hpp"

/* Compatibility macros for Python 3 */
#if PY_VERSION_HEX >= 0x03000000

#define PyInt_Check(x) PyLong_Check(x)
#define PyInt_AsLong(x) PyLong_AsLong(x)
#define PyInt_FromLong(x) PyLong_FromLong(x)
#define PyInt_FromSize_t(x) PyLong_FromSize_t(x)
#define PyString_Check(name) PyBytes_Check(name)
#define PyString_FromString(x) PyUnicode_FromString(x)
#define PyString_Format(fmt, args)  PyUnicode_Format(fmt, args)
#define PyString_AsString(str) PyBytes_AsString(str)
#define PyString_Size(str) PyBytes_Size(str)
#define PyString_InternFromString(key) PyUnicode_InternFromString(key)
#define Py_TPFLAGS_HAVE_CLASS Py_TPFLAGS_BASETYPE
#define PyString_AS_STRING(x) PyUnicode_AS_STRING(x)
#define _PyLong_FromSsize_t(x) PyLong_FromSsize_t(x)

#endif

class dbox_error: public std::logic_error {
public:
	dbox_error(const std::string& __arg) :
			std::logic_error(__arg) {
	}
};

/**
 * 初始化内部工作线程，不调用将不可用
 */
void initialize(int qsize);

/**
 * 退出内部工作线程，调用后将不可用
 */
void finalize();

struct VFileStat {
	unsigned int size { 0 };
	unsigned int mtime { 0 };
};

class VFileHelper {
private:
	std::shared_ptr<DVB::VFile> oFile;
	std::string sFile;
	std::shared_array_ptr<char> buffer;

	void validate() {
		if (oFile.get() == NULL) {
			throw dbox_error("Invalid VFile");
		}
	}
public:
	VFileHelper() {
	}

	VFileHelper(const std::shared_ptr<DVB::VFile> & file_) :
			oFile(file_) {
		sFile = file_->getFilename();
	}

	~VFileHelper() {
	}

	bool isValid() {
		return oFile.get() != NULL;
	}

	const std::string name() {
		return sFile;
	}

	const std::string message() {
		validate();
		return oFile->getMessage();
	}

	int Flush() {
		validate();
		return oFile->Flush();
	}

	ssize_t Read(char *& ptr, unsigned int size, unsigned int offset) {
		validate();
		if (size > MAX_REQUEST_SIZE) {
			throw dbox_error("Too large size: > 8MB");
		}

		ptr = buffer.resize(size);
		ssize_t bytes = oFile->Read(ptr, size, offset);

		if (bytes < 0) {
			throw dbox_error(oFile->getMessage());
		}

		return bytes;
	}

	ssize_t Read2(char *& ptr, unsigned int size, unsigned int offset) {
		validate();
		if (size > MAX_REQUEST_SIZE) {
			throw dbox_error("Too large size: > 8MB");
		}

		ptr = buffer.resize(size);
		ssize_t bytes = oFile->Read2(ptr, size, offset);

		if (bytes < 0) {
			throw dbox_error(oFile->getMessage());
		}

		return bytes;
	}

	ssize_t Write(const void * buffer, unsigned int size, unsigned int offset, bool async_write = false) {
		validate();
		if (size > MAX_REQUEST_SIZE) {
			throw dbox_error("Too large size: > 8MB");
		}

		ssize_t bytes = oFile->Write(buffer, size, offset, async_write);
		if (bytes < 0) {
			throw dbox_error(oFile->getMessage());
		}

		return bytes;
	}

	int Truncate(unsigned int newsize) {
		validate();
		return oFile->Truncate(newsize);
	}

	VFileStat * GetAttr() {
		validate();
		DVB::FileStat vstat;
		if (oFile->GetAttr(vstat) < 0) {
			throw dbox_error(oFile->getMessage());
		}

		VFileStat * stat = new VFileStat();
		stat->mtime = vstat.mtime;
		stat->size = vstat.size;

		return stat;
	}
};

class ClientHelper {
private:
	std::shared_ptr<DVB::DboxSlabClient> oClient;

public:
	ClientHelper(const char * hostinfo, int timeout = 30);

	~ClientHelper();

	VFileHelper * Open(const char * filename, const char * mode = NULL);

	int Unlink(const char * filename);

	int MkDir(const char * filename);

	int RmDir(const char * filename);

	VFileStat * GetAttr(const char * filename);

	int Truncate(const char * filename, unsigned int newsize);

	int Close(const char * filename);

};

#endif /* DBOXSLAB_SWIG_HPP_ */
