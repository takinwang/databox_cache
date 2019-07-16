#include <string>
#include <sstream>
#include <vector>

#include <databox/cpl_debug.h>
#include <databox/stringutils.hpp>

#include "dboxslab_swig.hpp"

void initialize(int qsize) {
	DVB::DboxSlabClient::initialize(qsize < 2 ? 2 : qsize);
}

void finalize() {
	DVB::DboxSlabClient::finalize();
}

ClientHelper::ClientHelper(const char* hostinfo, int timeout) {
	oClient = std::make_shared<DVB::DboxSlabClient>(hostinfo, timeout);
}

ClientHelper::~ClientHelper() {
	oClient.reset();
}

VFileHelper * ClientHelper::Open(const char* filename, const char* mode) {
	const auto & vfile = oClient->Open(filename, mode == NULL ? "r" : mode);
	return new VFileHelper(vfile);
}

int ClientHelper::Unlink(const char* filename) {
	std::string message;
	int state = oClient->Unlink(filename, message);
	if (state < 0) {
		throw dbox_error(message);
	}
	return state;
}

int ClientHelper::MkDir(const char* filename) {
	std::string message;
	int state = oClient->MkDir(filename, message);
	if (state < 0) {
		throw dbox_error(message);
	}
	return state;
}

int ClientHelper::RmDir(const char* filename) {
	std::string message;
	int state = oClient->RmDir(filename, message);
	if (state < 0) {
		throw dbox_error(message);
	}
	return state;
}

VFileStat* ClientHelper::GetAttr(const char* filename) {
	std::string message;
	DVB::FileStat vstat;
	int state = oClient->GetAttr(filename, vstat, message);
	if (state < 0) {
		throw dbox_error(message);
	}

	VFileStat * stat = new VFileStat();
	stat->mtime = vstat.mtime;
	stat->size = vstat.size;

	return stat;
}

int ClientHelper::Truncate(const char* filename, unsigned int newsize) {
	std::string message;
	int state = oClient->Truncate(filename, newsize, message);
	if (state < 0) {
		throw dbox_error(message);
	}
	return state;
}

int ClientHelper::Close(const char* filename) {
	std::string message;
	int state = oClient->Close(filename, message);
	if (state < 0) {
		throw dbox_error(message);
	}
	return state;
}
