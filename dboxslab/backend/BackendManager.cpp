/*
 * BackendManager.cpp
 *
 *  Created on: Nov 22, 2018
 *      Author: root
 */

//#define STATIC_BACKEND_PLUGINS

#include "BackendManager.hpp"

#ifdef STATIC_BACKEND_PLUGINS
#include <rados/rados_backend.hpp>
#include <local/local_backend.hpp>
#endif

#include <header.h>

BackendManager::BackendManager(const ConfReader & conf_) :
		oBackends( MAXBACKENDS) {

#ifdef STATIC_BACKEND_PLUGINS
	{
		std::shared_ptr<BackendFactory> oBackendFactory(new LocalBackendFactory("/etc/dboxslab/local.conf"));
		if (oBackendFactory->Prefix().size() > 0 && oBackendFactorys.count(oBackendFactory->Prefix()) == 0) {
			oBackendFactorys[oBackendFactory->Prefix()] = oBackendFactory;
		}
	}

	{
		std::shared_ptr<BackendFactory> oBackendFactory(new RadosBackendFactory("/etc/dboxslab/rados.conf"));
		oBackendFactorys[oBackendFactory->Prefix()] = oBackendFactory;
		if (oBackendFactory->Prefix().size() > 0 && oBackendFactorys.count(oBackendFactory->Prefix()) == 0) {
			oBackendFactorys[oBackendFactory->Prefix()] = oBackendFactory;
		}
	}
#else
	std::vector<std::string> plugins;
	conf_.get_strings("backend_plugin", ",", plugins);

	for (std::string & plugin_name : plugins) {
		if (plugin_name.empty() == true) {
			continue;
		}

		std::string plugin_filename = conf_.get_string(plugin_name, "");
		std::string plugin_confname = conf_.get_string(plugin_name + "_conf", "");

		if (plugin_filename.empty() == true || plugin_confname.empty() == true) {
			continue;
		}

		std::shared_ptr<PluginInfo> plugin_info(new PluginInfo(plugin_filename));
		if (plugin_info->valid() == false) {
			LOGGER_WARN(
					"#" << __LINE__ << ", BackendManager::BackendManager, Invalid backend plugin: " << plugin_filename)
			continue;
		}

		CreateBackend create_p = reinterpret_cast<CreateBackend>(plugin_info->get_symbol("create_backend"));

		if (create_p) {
			std::shared_ptr<BackendFactory> oBackendFactory = create_p(plugin_confname);
			if (oBackendFactory->Prefix().size() > 0 && oBackendFactorys.count(oBackendFactory->Prefix()) == 0) {
				LOGGER_TRACE(
						"#" << __LINE__ << ", BackendManager::BackendManager, Add BackendFactory: " << oBackendFactory->Prefix() << " from " << plugin_filename)
				oBackendFactorys[oBackendFactory->Prefix()] = oBackendFactory;
				oPluginInfos.push_back(plugin_info);
				continue;
			}

			LOGGER_WARN(
					"#" << __LINE__ << ", BackendManager::BackendManager, Duplicated BackendFactory: " << oBackendFactory->Prefix() << " from " << plugin_filename)
		}
	}
#endif

	for (const auto & iter : oBackendFactorys) {
		iter.second->Start();
	}

}

BackendManager::~BackendManager() {
	for (const auto iter : oBackendFactorys) {
		iter.second->Stop();
	}
}

std::shared_ptr<BackendFactory> BackendManager::GetBackendFactory(const std::string & prefix) {
	auto iter = oBackendFactorys.find(prefix);
	if (iter == oBackendFactorys.end()) {
		return oNullFactory;
	}
	return iter->second;
}

std::shared_ptr<Backend> BackendManager::Open(const std::string & filename, bool create_if_not_exists) {
	std::shared_ptr<Backend> oBackend;
	if (oBackends.get(filename, oBackend) == true) {
		return oBackend;
	}

	std::string prefix;
	std::string name;

	if (Parse(filename, prefix, name) == false) {
		return oNullBackend;
	}

	if (prefix.empty() == true || name.empty() == true) {
		return oNullBackend;
	}

	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory.get() == NULL) {
		return oNullBackend;
	}

	name = FileSystemUtils::NormalizePath(name);

	oBackend = oFactory->Open(name);
	oBackend->oBackendFactory = oFactory;

	if (oBackend->isValid(create_if_not_exists)) {
		oBackends.put(filename, oBackend, 0);
	}

	return oBackend;
}

bool BackendManager::Parse(const std::string& filename, std::string& prefix, std::string& name) {
	std::string::size_type pos = filename.find("://");

	if (pos == std::string::npos) {
		return false;
	}

	prefix = filename.substr(0, pos);
	name = filename.substr(pos + 3);

	return true;
}

bool BackendManager::IsMemory(const std::string& filename) {
	return filename.find( MEM_PREFIX) == 0;
}

bool BackendManager::Unlink(const std::string& filename, int & code, std::string & message) {

	std::shared_ptr<Backend> oBackend;
	std::string prefix;
	std::string name;

	if (GetBackend(filename, prefix, name, oBackend, code, message) == false) {
		return false;
	}

	/**
	 * oFactory 一定有
	 */
	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory->isReadOnly() == true) {
		errno = code = EROFS;
		message = strerror(code);
		return false;
	}

	oBackends.erase(filename);

	LOGGER_TRACE("#" << __LINE__ << ", BackendManager::Unlink: " << name);
	if (oBackend->Unlink(name) == false) {
		code = oBackend->code;
		message = oBackend->message;
		return false;
	}
	return true;
}

bool BackendManager::MkDir(const std::string& filename, int & code, std::string & message) {
	std::shared_ptr<Backend> oBackend;
	std::string prefix;
	std::string name;

	if (GetBackend(filename, prefix, name, oBackend, code, message) == false) {
		return false;
	}

	/**
	 * oFactory 一定有
	 */
	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory->isReadOnly() == true) {
		errno = code = EROFS;
		message = strerror(code);
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", BackendManager::MkDir: " << name);
	if (oBackend->MkDir(name) == false) {
		code = oBackend->code;
		message = oBackend->message;
		return false;
	}
	return true;
}

bool BackendManager::RmDir(const std::string& filename, int & code, std::string & message) {
	std::shared_ptr<Backend> oBackend;
	std::string prefix;
	std::string name;

	if (GetBackend(filename, prefix, name, oBackend, code, message) == false) {
		return false;
	}

	/**
	 * oFactory 一定有
	 */
	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory->isReadOnly() == true) {
		errno = code = EROFS;
		message = strerror(code);
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", BackendManager::RmDir: " << name);
	if (oBackend->RmDir(name) == false) {
		code = oBackend->code;
		message = oBackend->message;
		return false;
	}
	return true;
}

bool BackendManager::Truncate(const std::string& filename, off_t length, int & code, std::string & message) {
	std::shared_ptr<Backend> oBackend;
	std::string prefix;
	std::string name;

	if (GetBackend(filename, prefix, name, oBackend, code, message) == false) {
		return false;
	}

	/**
	 * oFactory 一定有
	 */
	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory->isReadOnly() == true) {
		errno = code = EROFS;
		message = strerror(code);
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", BackendManager::Truncate: " << name);
	if (oBackend->Truncate(name, length) == false) {
		code = oBackend->code;
		message = oBackend->message;
		return false;
	}
	return true;
}

bool BackendManager::GetAttr(const std::string& filename, struct stat* st, int & code, std::string& message) {
	std::shared_ptr<Backend> oBackend;
	std::string prefix;
	std::string name;

	if (GetBackend(filename, prefix, name, oBackend, code, message) == false) {
		return false;
	}

	LOGGER_TRACE("#" << __LINE__ << ", BackendManager::GetAttr: " << name);
	if (oBackend->GetAttr(name, st) == false) {
		code = oBackend->code;
		message = oBackend->message;
		return false;
	}

	if (st->st_size < 0) {
		code = EIO;
		message = strerror(code);
		return false;
	}

	return true;
}

bool BackendManager::GetBackend(const std::string& filename, std::string & prefix, std::string & name,
		std::shared_ptr<Backend>& oBackend, int & code, std::string& message) {

	if (Parse(filename, prefix, name) == false) {
		code = EINVAL;
		message = strerror(code);
		return false;
	}

	if (prefix.empty() == true || name.empty() == true) {
		code = EINVAL;
		message = strerror(code);
		return false;
	}

	name = FileSystemUtils::NormalizePath(name);
	if (oBackends.get(filename, oBackend) == true) {
		return true;
	}

	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory.get() == NULL) {
		code = EINVAL;
		message = strerror(code);
		return false;
	}

	oBackend = oFactory->Open(name);
	return true;
}

bool BackendManager::IsValid(const std::string& filename) {
	if (filename.find( MEM_PREFIX) == 0) { //内存文件
		return true;
	}

	std::string prefix;
	std::string name;

	if (Parse(filename, prefix, name) == false) {
		return false;
	}

	if (prefix.empty() == true || name.empty() == true) {
		return false;
	}

	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory.get() == NULL) {
		return false;
	}

	return oFactory->Validate(name);
}

void BackendManager::Close(const std::string& filename) {
	oBackends.erase(filename);
}

bool BackendManager::IsReadOnly(const std::string& filename) {
	if (filename.find( MEM_PREFIX) == 0) { //内存文件
		return false;
	}

	std::string prefix;
	std::string name;

	if (Parse(filename, prefix, name) == false) {
		return true;
	}

	if (prefix.empty() == true || name.empty() == true) {
		return true;
	}

	std::shared_ptr<BackendFactory> oFactory = GetBackendFactory(prefix);
	if (oFactory.get() == NULL) {
		return true;
	}

	return oFactory->isReadOnly();
}
