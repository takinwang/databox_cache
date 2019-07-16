/*
 * CacheFileService.hpp
 *
 *  Created on: Oct 15, 2018
 *      Author: root
 */

#ifndef CACHEFILESERVICE_HPP_
#define CACHEFILESERVICE_HPP_

#include <memory>
#include <databox/stringutils.hpp>
#include <databox/lrucache.hpp>

#include <databox/hbserver.hpp>
#include <databox/async_io_service.hpp>
#include <databox/thrdtimer.hpp>

#include <databox/cpl_debug.h>
#include <functional>

#include <header.h>

#include "CacheMetaManager.hpp"
#include "CacheStatusService.hpp"
#include "MetaLogger.hpp"

class CacheFileService: public std::enable_shared_from_this<CacheFileService> {
private:
	std::shared_ptr<AsyncIOService> oIoService;
	std::shared_ptr<MetaServerData> oServerData;

	std::shared_ptr<AsioTcpServer> oTcpServer;

	std::shared_ptr<MetaLogger> oMetaLogger;

	std::shared_ptr<MetaFileManager> oMetaFileManager;
	std::shared_ptr<DefaultStatusService> oCacheStatusService;
	bool bTerminating { false };
protected:

	ResultType DoSlabStatusPost(const std::shared_ptr<stringbuffer> & input,
			const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoSlabGetMeta(const std::shared_ptr<stringbuffer> & input, const std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoSlabPutMeta(const std::shared_ptr<stringbuffer> & input, const std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);
	/**
	 * 修改文件的修改时间和大小，只针对内存文件
	 */
	ResultType DoSlabPutAttr(const std::shared_ptr<stringbuffer> & input, const std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	/**
	 * 获取文件的修改时间和大小，只针对内存文件
	 */
	ResultType DoSlabGetAttr(const std::shared_ptr<stringbuffer> & input, const std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoSlabUnlinkFile(const std::shared_ptr<stringbuffer> & input,
			const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoSlabTruncateFile(const std::shared_ptr<stringbuffer> & input,
			const std::shared_ptr<stringbuffer> & output, const std::shared_ptr<asio_server_tcp_connection> & conn);
private:

	bool hb_replay_logs(const std::shared_ptr<MetaLogData> & logdata);

public:
	CacheFileService(const std::shared_ptr<MetaServerData>& serverdata_);

	~CacheFileService();

	ResultType hb_request(std::shared_ptr<stringbuffer> input, std::shared_ptr<stringbuffer> output,
			const boost::system::error_code &ec, std::shared_ptr<base_connection> conn);

	void run_server();

	void stop();

};

#endif /* CACHEFILESERVICE_HPP_ */
