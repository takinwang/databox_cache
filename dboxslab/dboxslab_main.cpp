#include <iostream>
#include <string>
#include <memory>
#include <sys/resource.h>

#include <databox/fork_watcher.hpp>
#include <databox/stringutils.hpp>
#include <databox/filesystemutils.hpp>

#include "w/SlabFileService.hpp"

#include <databox/cpl_debug.h>

#define MaxRLimits 65536 * 2

static void help(const char * app) {
	std::cerr << app << " [stop] <-H master_host> <-P master_port> <-p slab_port> [-s unix_socket]"
			<< " [-M memory_size] [-S swap_size] [--path swap_path] [-T bg_threads] [-d]" << std::endl;
	exit(-1);
}

static void error(const char * msg, const char * app) {
	std::cerr << msg << std::endl;
	help(app);
}

#define PIDFILE "/var/run/dboxslab.pid"

int run_master(int argc, char **argv, std::shared_ptr<SlabServerData> server_data) {

	std::string cnf_file = FileSystemUtils::FindFile("dboxslab.conf");

	ConfReader conf_;
	conf_.load(cnf_file);

	LOGGER_INFO(
			"#" << __LINE__ << ", run_master, memory_size: " << server_data->max_memory_slabs << ", swap_size: " << server_data->max_swap_slabs << ", swap_path: " << server_data->swap_path);

	std::shared_ptr<SlabFileService> tm(new SlabFileService(server_data, conf_));
	tm->run_server();

	tm.reset();

	usleep(100);
	return 0;
}

int main(int argc, char **argv) {
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

	std::shared_ptr<SlabServerData> oServerData(new SlabServerData);

	bool as_daemon = false;

	std::string memory_size_str = "500mb";
	std::string swap_size_str = "1000mb";

	oServerData->workers = std::thread::hardware_concurrency();

	for (int i = 1; i < argc; i++) {
		const char * a_arg = argv[i];
		if (STR_EQ(a_arg, "-H") && (i < argc - 1)) {
			oServerData->meta_addr = argv[++i];
		} else if (STR_EQ(a_arg, "-M") && (i < argc - 1)) {
			memory_size_str = argv[++i];
		} else if (STR_EQ(a_arg, "-T") && (i < argc - 1)) {
			oServerData->workers = StringUtils::ToLong(argv[++i], oServerData->workers);
		} else if (STR_EQ(a_arg, "-S") && (i < argc - 1)) {
			swap_size_str = argv[++i];
		} else if (STR_EQ(a_arg, "--path") && (i < argc - 1)) {
			oServerData->swap_path = argv[++i];
		} else if (STR_EQ(a_arg, "-p") && (i < argc - 1)) {
			oServerData->slab_port = StringUtils::ToLong(argv[++i], 6501); //块缓存服务 tcp 端口
		} else if (STR_EQ(a_arg, "-s") && (i < argc - 1)) {
			oServerData->slab_sock = argv[++i]; //块缓存服务 unix sock 文件
		} else if (STR_EQ(a_arg, "-P") && (i < argc - 1)) {
			oServerData->meta_port = StringUtils::ToLong(argv[++i], 6500); //远程服务端口
		} else if (STR_EQ(a_arg, "-d")) {
			as_daemon = true;
		} else {
			help(argv[0]);
		}
	}

	if (oServerData->meta_addr.empty() == true) {
		error("Empty master host!", argv[0]);
	}
	if (oServerData->meta_port == 0) {
		error("Empty master port!", argv[0]);
	}
	if (oServerData->slab_port == 0) {
		error("Empty local port!", argv[0]);
	}
	if (oServerData->swap_path.size() > 0) {
		if (FileSystemUtils::DirExists(oServerData->swap_path) == false) {
			error("Swap path not exist!", argv[0]);
		}

		uint64_t swap_size = 0;
		if (swap_size_str == "4/5") {
			swap_size = 0.75 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else if (swap_size_str == "3/4") {
			swap_size = 0.75 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else if (swap_size_str == "2/3") {
			swap_size = 0.66 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else if (swap_size_str == "1/2") {
			swap_size = 0.5 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else if (swap_size_str == "1/3") {
			swap_size = 0.33 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else if (swap_size_str == "1/4") {
			swap_size = 0.25 * SystemUtils::GetDiskFreeSize(oServerData->swap_path);
		} else {
			try {
				swap_size = StringUtils::ParseBytes(swap_size_str);
			} catch (...) {
				swap_size = 1000 * 1024 * 1024;
			}
		}

		if (swap_size == 0) {
			swap_size = 1000 * 1024 * 1024;
		}

		uint64_t slabs = swap_size / ( SIZEOFBLOCK * NUMBLOCKS);
		oServerData->max_swap_slabs = std::max<uint64_t>(2, slabs);
	}

	int64_t memory_size = SystemUtils::ParseMemory(memory_size_str);

	if (memory_size <= 0) {
		memory_size = 500 * 1024 * 1024;
	}

	uint64_t slabs = memory_size / ( SIZEOFBLOCK * NUMBLOCKS);
	oServerData->max_memory_slabs = std::max<uint64_t>(1, slabs);

	daemon_stop(PIDFILE);

	if (as_daemon == true) {
		daemon_init(PIDFILE);
		daemon_fork_children(1);
	}

#ifndef __NOLOGGER__
	std::string log_file = FileSystemUtils::FindFile("dboxslab.logger");
	LOGGER_INIT(log_file);
#endif

	return run_master(argc, argv, oServerData);
}
