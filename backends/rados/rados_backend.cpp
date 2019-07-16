#include "rados_backend.hpp"

#ifndef STATIC_BACKEND_PLUGINS

extern "C" std::shared_ptr<BackendFactory> create_backend(const std::string & conf) {
	std::shared_ptr<BackendFactory> backend(new RadosBackendFactory(conf));
	return backend;
}

extern "C" void on_plugin_load() {

}

extern "C" bool on_plugin_unload() {
	return false;
}

#endif
