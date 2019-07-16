#include <iostream>
#include <string>
#include <memory>

#include <databox/cpl_debug.h>
#include <databox/stringutils.hpp>

#include "CacheClient.hpp"

int main(int argc, char **argv) {
	std::string ws_address = "127.0.0.1:6501";
//	std::string ws_address = "/tmp/dbox.sock";
	std::string filename = "mem:///tmp/abc.txt";
	int offset = 0;

	DVB::DboxSlabClient::initialize(8);

	std::shared_array_ptr<unsigned char> buffer(1024 * 4 * 1024);
	unsigned char * ptr = buffer.get();

	std::shared_ptr<DVB::DboxSlabClient> tm(new DVB::DboxSlabClient(ws_address, 60));

//	{
//		std::shared_ptr<DVB::VFile> oCacheFile = tm->Open(filename, "w");
//		const char *fname = "/mnt/abc.txt"; // "/mnt/2018.tif";
//
//		mode_t mode = S_IRUSR | S_IWUSR; //| S_IRGRP | S_IROTH;
//		int fd = open(fname, O_RDONLY, mode);
//		if (fd < 0) {
//			std::cout << "no such file: " << fname << std::endl;
//			return -1;
//		}
//
//		off_t offset = 0;
//
//		while (true) {
//			ssize_t bytes_readed = read(fd, ptr, buffer.size());
//			if (bytes_readed <= 0) {
//				break;
//			}
//
//			ssize_t bytes = oCacheFile->Write(ptr, bytes_readed, offset);
//			if (bytes > 0) {
//				std::cout << oCacheFile->getMessage() << " " << bytes << " bytes" << std::endl;
//			} else {
//				std::cout << oCacheFile->getMessage() << std::endl;
//				break;
//			}
//			offset += bytes_readed;
//		}
//	}

	std::shared_ptr<DVB::VFile> oCacheFile = tm->Open(filename, "r");

	DVB::FileStat fsta;
	oCacheFile->GetAttr(fsta);

	std::cout << fsta.mtime << ", " << fsta.size << std::endl;

	{

		offset = 12;
		ssize_t bytes1 = oCacheFile->Read(ptr, buffer.size(), offset);

		if (bytes1 >= 0) {
			std::cout << bytes1 << " bytes" << std::endl;
		} else {
			std::cout << oCacheFile->getMessage() << std::endl;
		}
	}

	{
		offset = 12;
		ssize_t bytes1 = oCacheFile->Read2(ptr, buffer.size(), offset);

		if (bytes1 >= 0) {
			std::cout << bytes1 << " bytes" << std::endl;
		} else {
			std::cout << oCacheFile->getMessage() << std::endl;
		}
	}

	{
		offset = 12;
		ssize_t bytes1 = oCacheFile->Read2(ptr, buffer.size(), offset);

		if (bytes1 >= 0) {
			std::cout << bytes1 << " bytes" << std::endl;
		} else {
			std::cout << oCacheFile->getMessage() << std::endl;
		}
	}

	DVB::DboxSlabClient::finalize();

	return 0;
}
