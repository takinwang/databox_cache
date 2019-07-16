/*
 * BackendManager.hpp
 *
 *  Created on: Oct 13, 2018
 *      Author: root
 */

#ifndef BACKEND_MANAGER_HPP_
#define BACKEND_MANAGER_HPP_

#include <string>
#include <map>

#include <databox/cpl_conf.hpp>
#include <databox/filesystemutils.hpp>
#include <databox/stringutils.hpp>
#include <databox/lrucache.hpp>
#include <databox/cpl_dllplugin.hpp>

#include <backend.hpp>

using CreateBackend =std::shared_ptr<BackendFactory> (*) ( const std::string & conf );

#define MAXBACKENDS 1024 * 64
//最大可打开backend句柄数

/**
 * TODO 针对相同文件的多并发快速操作会有问题
 */

class BackendManager {
private:
	std::map<std::string, std::shared_ptr<BackendFactory> > oBackendFactorys;
	std::vector<std::shared_ptr<PluginInfo> > oPluginInfos;

	std::shared_ptr<Backend> oNullBackend;
	std::shared_ptr<BackendFactory> oNullFactory;

	cache::concurrent_lru_cache_count_num<std::shared_ptr<Backend> > oBackends;
protected:
	bool GetBackend(const std::string& filename, std::string & prefix, std::string & name,
			std::shared_ptr<Backend>& oBackend, int & code, std::string& message);

public:

	BackendManager(const ConfReader & conf_);

	virtual ~BackendManager();

	std::shared_ptr<BackendFactory> GetBackendFactory(const std::string & prefix);

	/**
	 * 以 readonly == false 方式打开，如果底部没有文件，会创建一个 空文件，readonly == true 也可以写入数据
	 */
	std::shared_ptr<Backend> Open(const std::string & filename, bool create_if_not_exists);

	bool Unlink(const std::string & filename, int & code, std::string & message);

	bool MkDir(const std::string & filename, int & code, std::string & message);

	bool RmDir(const std::string & filename, int & code, std::string & message);

	bool Truncate(const std::string& filename, off_t length, int & code, std::string & message);

	bool Parse(const std::string & filename, std::string & prefix, std::string & name);

	bool GetAttr(const std::string& filename, struct stat * st, int & code, std::string & message);

	bool IsMemory(const std::string & filename);

	bool IsValid(const std::string & filename);

	bool IsReadOnly(const std::string & filename);

	void Close(const std::string & filename);

};

#endif /* BACKEND_MANAGER_HPP_ */
