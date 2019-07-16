/*
 * SlabFileService.hpp
 *
 *  Created on: Oct 16, 2018
 *      Author: root
 */

#ifndef SLABFILESERVICE_HPP_
#define SLABFILESERVICE_HPP_

#include <memory>
#include <databox/stringutils.hpp>
#include <databox/lrucache.hpp>

#include <databox/hbserver.hpp>
#include <databox/async_io_service.hpp>
#include <databox/thrdtimer.hpp>

#include <databox/cpl_debug.h>

#include <json/json.h>
#include <functional>

#include <header.h>

#include "backend/BackendManager.hpp"

#include "SlabFileManager.hpp"
#include "memory/SlabMemManager.hpp"
#include "memory/SlabMMapManager.hpp"

class SlabFileService: public std::enable_shared_from_this<SlabFileService> {
private:
	std::shared_ptr<AsyncIOService> oIoService;
	std::shared_ptr<SlabServerData> oServerData;

	std::shared_ptr<AsioTcpServer> oSlabTcpServer;
	std::shared_ptr<AsioUnixServer> oSlabUnixServer;

	std::shared_ptr<SlabFactory> oSlabFactory;
	std::shared_ptr<BackendManager> oBackendManager;

	std::shared_ptr<SlabFileManager> oSlabFileManager;

	bool bTerminating { false };

protected:

	ResultType DoClientRead(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientRead2(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientWrite(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientUnlink(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientRmDir(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientMkDir(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientGetAttr(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientTruncate(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientClose(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoClientFlush(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoSlabPeerRead(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer> & output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

//	ResultType DoClientAdmin(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
//			const std::shared_ptr<asio_server_tcp_connection> & conn);

	ResultType DoMasterCheckIt(const std::shared_ptr<stringbuffer> & input, std::shared_ptr<stringbuffer>& output,
			const std::shared_ptr<asio_server_tcp_connection> & conn);

public:

	SlabFileService(const std::shared_ptr<SlabServerData>& serverdata_, const ConfReader & conf_);

	~SlabFileService();

	ResultType hb_request(const std::shared_ptr<stringbuffer>& input, std::shared_ptr<stringbuffer>& output,
			const boost::system::error_code &ec, std::shared_ptr<base_connection> conn);

	void run_server();

	void stop();
};

#endif /* SLABFILESERVICE_HPP_ */
