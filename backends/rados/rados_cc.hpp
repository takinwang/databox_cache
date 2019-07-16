/*
 * rados_cc.hpp
 *
 *  Created on: Mar 14, 2018
 *      Author: root
 */

#ifndef RADOS_CC_HPP_
#define RADOS_CC_HPP_

#include <string>
#include <memory>

#include <errno.h>

#include <rados/librados.h>

#include <databox/cpl_conf.hpp>
#include <databox/cpl_debug.h>

struct RadosContext {
private:
	rados_ioctx_t ioctx_ { NULL };
	rados_completion_t comp_ { NULL };

	rados_t cluster_ { NULL };
	std::string pool_;

	bool has_data_ { false };
public:
	RadosContext(rados_t cluster, const std::string & pool) :
			cluster_(cluster), pool_(pool) {
		LOGGER_TRACE("#" << __LINE__ << ", RadosContext::RadosContext, rados_ioctx_create: " << pool_);
		int err = 0;
		if ((err = rados_ioctx_create(cluster_, pool_.c_str(), &ioctx_)) < 0) {
			errno = abs(err);
			return;
		};
	}

	~RadosContext() {
		aio_close();

		if (ioctx_) {
			LOGGER_TRACE("#" << __LINE__ << ", RadosContext::~RadosContext, rados_ioctx_destroy: " << pool_);
			rados_ioctx_destroy(ioctx_);
			ioctx_ = NULL;
		}
	}

	int aio_start() {
		aio_close();
		LOGGER_TRACE("#" << __LINE__ << ", RadosContext::aio_start, rados_aio_create_completion: " << pool_);
		int err = rados_aio_create_completion(NULL, NULL, NULL, &comp_);
		if (err < 0) {
			errno = abs(err);
		}
		return err;
	}

	int aio_flush() { // 当有数据写入过，需要进行 flush
		if (comp_ == NULL || has_data_ == false) {
			return 0;
		}

		if (ioctx_) {
			LOGGER_TRACE("#" << __LINE__ << ", RadosContext::aio_flush, rados_aio_flush: " << pool_);
			int err = rados_aio_flush(ioctx_);
			if (err < 0) {
				errno = abs(err);
			}
			return err;
		}

		has_data_ = false;
		return 0;
	}

	int aio_write(const char *oid, const char *buf, size_t len, uint64_t off) {
		has_data_ = true;

		if (comp_ == NULL) {
			LOGGER_TRACE(
					"#" << __LINE__ << ", RadosContext::aio_write, rados_write: " << pool_ << "|" << std::string(oid) << ", size: " << len << ", offset: " << off);
			int err = rados_write(ioctx_, oid, buf, len, off);
			if (err < 0) {
				errno = abs(err);
			}
			return err;
		}

		LOGGER_TRACE(
				"#" << __LINE__ << ", RadosContext::aio_write, rados_aio_write: " << pool_ << "|" << std::string(oid) << ", size: " << len << ", offset: " << off);
		int err = rados_aio_write(ioctx_, oid, comp_, buf, len, off);
		if (err < 0) {
			errno = abs(err);
		}
		return err;
	}

	int aio_close() {
		if (comp_ == NULL) {
			return 0;
		}

		int err = 0;
		if (ioctx_) {
			LOGGER_TRACE("#" << __LINE__ << ", RadosContext::aio_flush, rados_aio_flush: " << pool_);
			err = rados_aio_flush(ioctx_);
			if (err < 0) {
				errno = abs(err);
			}
		}

		rados_aio_release(comp_);
		comp_ = NULL;

		has_data_ = false;
		return err;
	}

	rados_t cluster() const {
		return cluster_;
	}

	rados_ioctx_t ioctx() const {
		return ioctx_;
	}

	const std::string& pool() const {
		return pool_;
	}
};

class CephNode {
private:
	rados_t rados_ { NULL };

	std::string ceph_conf_; //"/etc/ceph/ceph.conf"
	std::string user_name_; //= "admin";
	std::string ceph_name_; //ceph0

	bool connected_ { false };
public:
	CephNode(const std::string & ceph_name, const std::string & ceph_conf, const std::string & user_name) :
			ceph_conf_(ceph_conf), user_name_(user_name), ceph_name_(ceph_name) {
//		LOGGER_TRACE("CephNode::CephNode(), " << ceph_name_);
	}

	~CephNode() {
		if (rados_ != NULL) {
			rados_shutdown(rados_);
		}

//		LOGGER_TRACE("CephNode::~CephNode(), " << ceph_name_);
	}

	std::shared_ptr<RadosContext> ctx(const std::string & pool) {
//		TRACE_TIMER(__stats__, "CephNode::ctx()")
		std::shared_ptr<RadosContext> ctx = std::make_shared<RadosContext>(rados_, pool);
		return ctx;
	}

	rados_t rados() {
		return rados_;
	}

	bool connected() const {
		return connected_;
	}

	int connect() {
		if (connected_ == true) {
			return 0;
		}

		TRACE_TIMER(__stats__, "CephNode::connect()")

		int err = 0;
		err = rados_create(&rados_, user_name_.c_str());
		if (err < 0) {
			errno = abs(err);
			return err;
		}

		err = rados_conf_read_file(rados_, ceph_conf_.c_str());
		if (err < 0) {
			errno = abs(err);
			return err;
		}

		err = rados_connect(rados_);
		if (err < 0) {
			errno = abs(err);
			return err;
		}

		connected_ = true;
		return err;
	}

	const std::string& name() const {
		return ceph_name_;
	}
};

class VsiCephClient {
private:
	std::map<std::string, std::shared_ptr<CephNode> > cluster_;
public:
	VsiCephClient(const std::string & conf) {

//		LOGGER_TRACE("VsiCephClient::VsiCephClient: " << conf);

		ConfReader cr;
		cr.load(conf);

//		 /etc/vsirados.conf
		// cluster= ceph0,ceph1

		// ceph0_user=admin
		// ceph0_conf=/etc/ceph/ceph.conf

		// ceph1_user=admin
		// ceph1_conf=/etc/ceph/ceph.conf

		std::vector<std::string> keys;
		cr.get_strings("cluster", ",", keys);

		for (std::string & key : keys) {
			std::string ceph_conf_ = cr.get_string(key + "_conf", "");
			std::string user_name_ = cr.get_string(key + "_user", "admin");

			if (ceph_conf_.size() > 0) {
				cluster_[key] = std::make_shared<CephNode>(key, ceph_conf_, user_name_);
			}
		}
	}

	~VsiCephClient() {
//		LOGGER_TRACE("~VsiCephClient()");
	}

	int connect(const std::string & ceph, std::shared_ptr<CephNode> & node) {
		node = find(ceph);
		if (node.get() != NULL) {
			return node->connect();
		}
		return -1;
	}

	std::shared_ptr<CephNode> find(const std::string & ceph) {
		auto iter = cluster_.find(ceph);
		if (iter == cluster_.end()) {
			return std::shared_ptr<CephNode>();
		}
		return iter->second;
	}
};

#endif /* RADOS_CC_HPP_ */
