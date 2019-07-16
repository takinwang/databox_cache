/*
 * ServiceStatus.hpp
 *
 *  Created on: Dec 2, 2018
 *      Author: root
 */

#ifndef M_SERVICESTATUS_HPP_
#define M_SERVICESTATUS_HPP_

#include <databox/libhttp/server_http.hpp>

#include "CacheMetaManager.hpp"

class ServiceStatus {

public:
	virtual ~ServiceStatus() {
	}

	virtual std::string Action() = 0;

	virtual std::string Process(const std::shared_ptr<MetaFileManager>& oMetaFileManager,
			SimpleWeb::CaseInsensitiveMultimap & oQuerys, SimpleWeb::CaseInsensitiveMultimap & oHeader) = 0;
};

#endif /* M_SERVICESTATUS_HPP_ */
