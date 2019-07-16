/*
 * Server.h
 *
 *  Created on: Oct 31, 2018
 *      Author: root
 */

#ifndef SERVER_H_
#define SERVER_H_

#include <string>
#include <stdlib.h>
#include <stdexcept>

#include <databox/stringutils.hpp>

struct MetaServerData {

	std::string address { "0.0.0.0" };

	// 元数据服务 tcp 端口
	uint16_t meta_port { 6500 };

	// 状态服务接口，http 协议
	uint16_t status_port { 6550 };

	// 本地服务线程数量
	uint16_t workers { 4 };

	// 内存最大文件数，超过会被释放
	uint32_t max_files { 2 * 256 * 1024 }; //

	/**
	 * 日志文件路径
	 */
	std::string meta_path;
};

struct SlabServerData {

	std::string meta_addr { "dboxcache" };

	//块缓存服务 tcp 端口
	uint16_t slab_port { 6501 };

	// 块缓存服务 unix sock 文件
	std::string slab_sock;

	// 元数据服务 tcp 端口
	uint16_t meta_port { 6500 };

	// 本地服务线程数量
	uint16_t workers { 4 };

	//是否可以离线使用
	bool enable_offline { true };

	/**
	 * 内存块缓存数量
	 */
	uint32_t max_memory_slabs { 10 };

	/**
	 * 磁盘块缓存路径，不为空，则必须为目录
	 */
	std::string swap_path;

	/**
	 * 磁盘块缓存数量，必须大于 max_memory_slabs * 2 才会生效
	 */
	uint32_t max_swap_slabs { 100 };

	uint32_t meta_ttl { 5  * 60 }; //元数据缓存时间
	uint32_t stat_ttl { 10 * 60 }; //文件属性缓存时间

	// 内存最大文件数，超过会被释放
	uint32_t max_files { 1024 * 1024 };
};

class SlabPeer {
protected:
	std::string host;
	unsigned short int port { 0 };
public:
	SlabPeer(const std::string & host_, unsigned short int port_) :
			host(host_), port(port_) {
	}

	SlabPeer(const std::string & peer) {
		std::string s_port;
		if (StringUtils::SplitHostPort(peer, host, s_port) == true) {
			STR_TO_LONG(port, s_port, 0);
		}
	}

	const std::string getKey() const {
		return host + ":" + StringUtils::ToString(port);
	}

	const std::string& getHost() const {
		return host;
	}

	unsigned short int getPort() const {
		return port;
	}
};

#endif /* SERVER_H_ */
