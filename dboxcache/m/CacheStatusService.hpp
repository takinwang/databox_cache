/*
 * DefaultStatusService.hpp
 *
 *  Created on: Dec 2, 2018
 *      Author: root
 */

#ifndef M_CACHESTATUSSERVICE_HPP_
#define M_CACHESTATUSSERVICE_HPP_

#define URL_PREFIX "/+databox/+dboxcache"
#define HEADER_CHARSET "; charset=utf-8"

#define VALIDATE_LENGTH( var, len ) { \
	/* 防止恶意参数 */\
	if(var.size() > len) {\
		response->write( SimpleWeb::StatusCode::client_error_bad_request );\
		return;\
	}\
}

#include <map>
#include <memory>

#include "ServiceStatus.hpp"
#include "CacheMetaManager.hpp"

#include <databox/cpl_debug.h>
#include <databox/async_io_service.hpp>

typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;

#define STATUS_TIMEOUT 60

class DefaultStatusService {
private:
	uint16_t ws_port;

	std::shared_ptr<AsyncIOService> oIoService;

	std::shared_ptr<HttpServer> oHttpServer;

	std::shared_ptr<MetaFileManager> oMetaFileManager;

	/**
	 * 只读 map，线程安全
	 */
	std::map<std::string, std::shared_ptr<ServiceStatus> > oServiceStatusMap;
private:
	void define_services();

	void define_status();

public:
	DefaultStatusService(const std::shared_ptr<MetaFileManager>& oMetaFileManager_, uint16_t uint16_t);

	void start();

	void stop();

};

#endif /* M_CACHESTATUSSERVICE_HPP_ */
