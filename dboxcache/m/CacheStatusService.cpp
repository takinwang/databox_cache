#include "CacheStatusService.hpp"
#include <databox/filesystemutils.hpp>

DefaultStatusService::DefaultStatusService(const std::shared_ptr<MetaFileManager>& oMetaFileManager_, uint16_t ws_port_) :
		ws_port(ws_port_), oMetaFileManager(oMetaFileManager_) {

	oIoService = AsyncIOService::getInstance(); // std::make_shared<AsyncIOService>();

	oHttpServer = std::make_shared<HttpServer>();
	oHttpServer->io_service = oIoService->getIoService();

	oHttpServer->config.max_request_streambuf_size = 4 * 1024 * 1024;
	oHttpServer->config.address = "0.0.0.0";
	oHttpServer->config.port = ws_port_;
	oHttpServer->config.timeout_request = 10;
	oHttpServer->config.timeout_content = 30;
	oHttpServer->config.reuse_address = true;

	oHttpServer->on_error = [this](std::shared_ptr<HttpServer::Request> request, const boost::system::error_code & ec) {
	};

}

void DefaultStatusService::start() {
	LOGGER_INFO("#" << __LINE__ << ", CacheStatusService::Accepting Status Request on port: " << ws_port);

	define_status();
	define_services();

	oHttpServer->start();

}

#include "status/ServiceStatusHome.hpp"

void DefaultStatusService::define_status() {
	{
		std::shared_ptr<ServiceStatus> s_status(new ServiceStatusHome);
		oServiceStatusMap[s_status->Action()] = s_status;
	}

}

void DefaultStatusService::stop() {
	oHttpServer->stop();
}

void DefaultStatusService::define_services() {

	oHttpServer->default_resource["GET"] =
			[this](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
				response->write( SimpleWeb::StatusCode::client_error_not_found );
			};

	std::string re_expr = URL_PREFIX "/+status/+(\\w+)$";

	auto callback =
			[ this ](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
				SimpleWeb::CaseInsensitiveMultimap oQuerys = request->parse_query_string();

				std::string action = StringUtils::Lower( request->path_match[1] );
				VALIDATE_LENGTH(action, 64)

				std::shared_ptr<ServiceStatus> oServiceStatus;
				GET_MAP_VAL_NODEF( oServiceStatus, oServiceStatusMap, action );

				if (oServiceStatus.get() == NULL) {
					response->write( SimpleWeb::StatusCode::client_error_not_found );
					return;
				}

				SimpleWeb::CaseInsensitiveMultimap oHeader;
				oHeader.emplace("Date", SystemUtils::gmt_time());

				std::string pretty;
				GET_MAP_VAL_NODEF( pretty, oQuerys, "pretty" );

				std::string data;
				try {
					data = oServiceStatus->Process( oMetaFileManager, oQuerys, oHeader );
					if(pretty == "1" || pretty == "yes") {
						data = StringUtils::PrettyJsonString(data);
					}
					data.append("\n");

				} catch( std::exception & e ) {
					response->write( SimpleWeb::StatusCode::server_error_internal_server_error );
					LOGGER_ERROR("CacheStatusService::define_services, Error: " << e.what() );
					return;
				}

				oHeader.emplace("Last-Modified", SystemUtils::gmt_time());
				oHeader.emplace("Cache-Control", "public, max-age=60");
				oHeader.emplace("Accept-Ranges", "bytes" );

				oHeader.emplace("Content-Type", "application/json" HEADER_CHARSET );

#ifndef __NO_ACCESS_CONTROL__
			oHeader.emplace("Access-Control-Allow-Origin", "*");
#endif
			response->write( data, oHeader );
		};

	LOGGER_TRACE("#" << __LINE__ << ", CacheStatusService::define_services, [GET], " << re_expr);
	oHttpServer->resource[re_expr]["GET"] = callback;
}
