#include <iostream>
#include <sys/resource.h>
#include <databox/fork_watcher.hpp>

#include "m/CacheFileService.hpp"

#define PIDFILE "/var/run/dboxcache.pid"

#define MaxRLimits 65536 * 2

static void help(const char * app) {
	std::cerr << app << " [stop] [-P master_port] [-T bg_threads] [-d] [--path meta_path]" << std::endl;
}

int main(int argc, char ** argv) {
	if (argc == 2 && STR_EQ(argv[1], "stop")) {
		daemon_stop( PIDFILE);
		return 0;
	}

	struct rlimit rt;
	/* 设置每个进程允许打开的最大文件数 */
	rt.rlim_max = rt.rlim_cur = MaxRLimits;

	if (setrlimit(RLIMIT_NOFILE, &rt) != 0) {
		std::cerr << "setrlimit error: " << MaxRLimits << std::endl;
	}

	std::shared_ptr<MetaServerData> oServerData(new MetaServerData);

	oServerData->meta_path = "/tmp/logs";

	oServerData->workers = std::thread::hardware_concurrency();

	bool as_daemon = false;
	for (int i = 1; i < argc; i++) {
		const char * a_arg = argv[i];
		if (STR_EQ(a_arg, "-P") && (i < argc - 1)) {
			oServerData->meta_port = StringUtils::ToLong(argv[++i], 6500);
		} else if (STR_EQ(a_arg, "-T") && (i < argc - 1)) {
			oServerData->workers = StringUtils::ToLong(argv[++i], oServerData->workers);
		} else if (STR_EQ(a_arg, "-d")) {
			as_daemon = true;
		} else if (STR_EQ(a_arg, "--path") && (i < argc - 1)) {
			oServerData->meta_path = argv[++i];
		} else {
			help(argv[0]);
			exit(0);
		}
	}
	if (oServerData->meta_port == 0) {
		help(argv[0]);
		return -1;
	}

	daemon_stop( PIDFILE);

	if (as_daemon == true) {
		daemon_init( PIDFILE);
		daemon_fork_children(1);
	}

#ifdef __NOLOGGER__
	std::string log_file = FileSystemUtils::FindFile("dboxcache.logger");
	LOGGER_INIT(log_file);
#endif

	std::shared_ptr<CacheFileService> cache_srv(new CacheFileService(oServerData));

	cache_srv->run_server();

	cache_srv.reset();

	usleep(100);
	return 0;
}
