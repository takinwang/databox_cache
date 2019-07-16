/*
 * ServiceStatusHome.hpp
 *
 *  Created on: Dec 2, 2018
 *      Author: root
 */

#ifndef M_STATUS_SERVICESTATUSHOME_HPP_
#define M_STATUS_SERVICESTATUSHOME_HPP_

#include <json/json.h>
#include <string>

#include <databox/stringutils.hpp>

#include "../ServiceStatus.hpp"

class ServiceStatusHome: public ServiceStatus {
public:

	std::string Action() {
		return "home";
	}

	std::string Process(const std::shared_ptr<MetaFileManager>& oMetaFileManager,
			SimpleWeb::CaseInsensitiveMultimap & oQuerys, SimpleWeb::CaseInsensitiveMultimap & oHeader);

};

inline std::string ServiceStatusHome::Process(const std::shared_ptr<MetaFileManager>& oMetaFileManager,
		SimpleWeb::CaseInsensitiveMultimap& oQuerys, SimpleWeb::CaseInsensitiveMultimap& oHeader) {
	std::string field;
	GET_MAP_VAL_NODEF(field, oQuerys, "c");
	return oMetaFileManager->GetStatus(field);
}

#endif /* M_STATUS_SERVICESTATUSHOME_HPP_ */
