/*
 * CacheMetaManager.cpp
 *
 *  Created on: Nov 22, 2018
 *      Author: root
 */

#include <databox/hbserver.hpp>
#include "CacheMetaManager.hpp"

#include "MetaFile.inc"

MetaFileManager::MetaFileManager(const std::shared_ptr<MetaServerData>& serverdata_,
		const std::shared_ptr<MetaLogger> & meta_logger_) :
		oServerData(serverdata_), oMetaFiles_m(serverdata_->max_files), oMetaLogger(meta_logger_) {

	LOGGER_TRACE("#" << __LINE__ << ", MetaFileManager::MetaFileManager, MaxFiles: " << oServerData->max_files);

	io_service = AsyncIOService::getInstance()->getIoService();

	timer_Nodes = std::make_shared<boost::asio::deadline_timer>(*io_service);
	timer_Trush = std::make_shared<boost::asio::deadline_timer>(*io_service);

	oSlabMessager = std::make_shared<TcpMessager>();
}

MetaFileManager::~MetaFileManager() {
	LOGGER_TRACE("#" << __LINE__ << ", MetaFileManager::~MetaFileManager");
}

/**
 * 利用消息队列进行消息发送到节点
 */
void MetaFileManager::PostMessage(const std::shared_ptr<TcpMessage> & message) {
	oSlabMessages.push_back(message);

	EnterMessageLoop();

	size_t nMessages = oSlabMessages.qsize();
	size_t nms = nMessages / 10;
	if (nms > 0) {
		//待刷新的数据过多，需要降低 PostMessage 速度
		LOGGER_TRACE("MetaFileManager::PostMessage, "<< nMessages << " items, slow down: " << nms << " ms")
		std::this_thread::sleep_for(std::chrono::milliseconds(nms));
	}
}

void MetaFileManager::EnterMessageLoop() {
	{
		std::lock_guard<std::mutex> lock(mtx_loop);
		if (bMessagerLoop == true) {
			return;
		}
		bMessagerLoop = true;
	}

	std::shared_ptr<TcpMessage> message;
	if (oSlabMessages.pop_front(message) == false || message.get() == NULL) {
		bMessagerLoop = false;
		return;
	}

	oSlabMessager->PostMessage(message);
}

void MetaFileManager::LeaveMessageLoop() {
	bMessagerLoop = false;
	EnterMessageLoop();
}

void MetaFileManager::Remove(const std::string& filename) {
	oMetaFiles_m.erase(filename);
}

/**
 * 在元数据上注册数据块，此函数为线程安全
 */

std::shared_ptr<MetaFile> MetaFileManager::GetOrCreate(const std::string& filename, int32_t uuid) {
	auto self = this->shared_from_this();
	std::shared_ptr<MetaFile> oMetaFile;

	oMetaFiles_m.get_or_create(filename, oMetaFile, 0, [ this, self, filename, uuid ]() {
		return std::make_shared<MetaFile>( self, filename, uuid );
	});

	if (oMetaFile.get()) {
		oMetaFile->Update();
	}

	return oMetaFile;
}

std::shared_ptr<MetaFile> MetaFileManager::Get(const std::string& filename) {
	std::shared_ptr<MetaFile> oMetaFile;
	oMetaFiles_m.get(filename, oMetaFile);

	if (oMetaFile.get()) {
		oMetaFile->Update();
	}

	return oMetaFile;
}

void MetaFileManager::TraceTrushes(float factor) {
	boost::system::error_code ec;
	timer_Trush->cancel(ec);

	size_t nFiles = oMetaFiles_m.size();

	float base_time = 32 * 1000;
	if (nFiles > 6400) {
		base_time = 1 * 1000;
	} else if (nFiles > 3200) {
		base_time = 2 * 1000;
	} else if (nFiles > 1600) {
		base_time = 4 * 1000;
	} else if (nFiles > 800) {
		base_time = 8 * 1000;
	} else if (nFiles > 400) {
		base_time = 16 * 1000;
	}

	base_time = base_time / 2;

	base_time *= factor;
	if (base_time < 10) {
		base_time = 10;
	}

	timer_Trush->expires_from_now(boost::posix_time::milliseconds(static_cast<int>(base_time)));

	LOGGER_TRACE(
			"#" << __LINE__ << ", MetaFileManager::TraceTrushes, " << nFiles << " files, Check Interval: " << base_time << " ms");

	auto self = this->shared_from_this(); /*  异步函数，通过自引用传递保持生命周期 */
	timer_Trush->async_wait(
			[ this, self, nFiles ](const boost::system::error_code &ec) {

				if(ec) {
					return;
				}

				std::string filename;
				std::shared_ptr<MetaFile> oMetaFile;

				/**
				 * 通过 gc 方式获取队列尾部对象，同时将对象放入队列头部
				 */
				oMetaFiles_m.gc( filename, oMetaFile, [ this, self ]( const std::shared_ptr<MetaFile> & gMetaFile ) {
							if(gMetaFile.get() == NULL ) {
								/**
								 * 没有缓存块，直接废掉
								 */
								return false;
							}

							return true;
						});

				if( oMetaFile.get() == NULL ) {
					TraceTrushes( 1.0 );
					return;
				}

				if( oMetaFile->GetNumMetaBlocks() == 0 ) {
					oMetaFiles_m.erase(filename);
					oMetaLogger->RemoveFile(filename);
					TraceTrushes( 0.1 );
					return;
				}

				if( oMetaFile->IsTimeout( FILE_TIMEOUT ) == false ) {
					TraceTrushes( 1.0 );
					return;
				}

				/**
				 * 获取当前文件的所有块信息，然后检查所有 peers，返回待删除的块编号列表
				 */
				std::vector< uint32_t > block_ids;
				oMetaFile->GetKeys( block_ids );

				LOGGER_TRACE("#" << __LINE__ << ", SlabFileManager::TraceTrushes: " << filename << ", Blocks: " << block_ids.size());

				std::vector< std::string > peers;
				oSlabPeers_m.get_keys(peers );

				for ( const std::string & peer : peers ) {
					std::shared_ptr<SlabPeerData> oSlabPeer;
					oSlabPeers_m.get( peer , oSlabPeer );

					if (oSlabPeer.get() == NULL) {
						continue;
					}

					std::shared_ptr<TcpMessage> message = std::make_shared<TcpMessage>();
					message->host = oSlabPeer->getHost();
					message->port = oSlabPeer->getPort();

					message->output->write_int8( CacheAction::caMasterCheckIt );
					message->output->write_int32( oMetaFile->GetUuid() );
					message->output->write_str( filename );

					message->output->write_uint32( block_ids.size() );
					for ( uint32_t block_id : block_ids ) {
						message->output->write_uint32( block_id );
					}

					message->callback = [ this, self, filename, peer, block_ids, oMetaFile ]( std::shared_ptr<stringbuffer> input,
							const boost::system::error_code & ec, std::shared_ptr<base_connection> conn1 ) {
						/* 网络故障处理，服务端会关闭连接，当前不做处理 */
						if(ec) {
							LeaveMessageLoop();
							LOGGER_INFO(
									"#" << __LINE__ << ", MetaFileManager::TraceTrushes: " << filename << ", Error: " << ec.message());
							return;
						}

						int8_t action;
						uint32_t nBlocks = 0;
						if (input->read_int8(action) == false || input->read_uint32(nBlocks) == false || action != CacheAction::caMasterCheckItResp) {
							/**
							 * 忽略错误
							 */
							LeaveMessageLoop();
							return;
						}

						int32_t uuid = oMetaFile->GetUuid();

						/**
						 * 节点上没有该文件，可以直接删除所有块
						 */
						if (nBlocks == 0 ) {
							for ( uint32_t block_id : block_ids) {
								oMetaFile->RemovePeer( block_id, peer );
								oMetaLogger->RemoveBlock( filename, uuid, block_id, peer );
							}

							if( oMetaFile->GetNumMetaBlocks() == 0 ) {
								oMetaFiles_m.erase( filename );
								oMetaLogger->RemoveFile(filename);
							} else {
								oMetaFile->Update();
							}

							LeaveMessageLoop();
							return;
						}

						for (uint32_t i = 0; i < nBlocks; i++) {
							uint32_t block_id;
							if (input->read_uint32(block_id) == false) {
								break;
							}
							/**
							 * 如果本地缓存没有该块，返回待删除块
							 */
							oMetaFile->RemovePeer( block_id, peer );
							oMetaLogger->RemoveBlock( filename, uuid, block_id, peer );
						}

						if( oMetaFile->GetNumMetaBlocks() == 0 ) {
							oMetaFiles_m.erase( filename );
							oMetaLogger->RemoveFile(filename);
						} else {
							oMetaFile->Update();
						}
						LeaveMessageLoop();
					};

					PostMessage( message );
				}

				TraceTrushes( 0.1 );
			});
}

void MetaFileManager::TraceNodes() {

	boost::system::error_code ec;
	timer_Nodes->cancel(ec);

	if (bTerminating == true) {
		return;
	}

	auto self = this->shared_from_this();

	timer_Nodes->expires_from_now(boost::posix_time::seconds(2));
	timer_Nodes->async_wait(
			[ this, self ](const boost::system::error_code &ec) {

				if(ec) {
					return;
				}

				std::map< std::string, std::shared_ptr<SlabPeerData> > tmp_peers;
				while (bTerminating == false ) {
					std::string peername;
					if ( oSlabPeerNames.pop_front( peername ) == false ) {
						break;
					}

					std::shared_ptr<SlabPeerData> oSlabPeerData;
					oSlabPeers_m.get( peername, oSlabPeerData );
					if ( oSlabPeerData.get() == NULL ) {
						continue;
					}

					if( oSlabPeerData->IsTimeout( NODE_TIMEOUT ) == true) {
						oSlabPeers_m.erase( peername );
						SYSLOG_WARN( "#" << __LINE__ << ", MetaFileManager::TraceNodes, Timeout, Remove: " << peername << ", Nodes: " << oSlabPeers_m.size());
						continue;
					}

					/**
					 * 可用的再利用
					 */
					if (tmp_peers.count( peername ) == 0) {
						tmp_peers[ peername ] = oSlabPeerData;
					}
					//					LOGGER_TRACE( "#" << __LINE__ << ", MetaFileManager::TraceNodes, Push back: " << peername );
				}

				/**
				 * 可用的再利用
				 */

				Json::Value oRoot ( Json::objectValue );
				{
					Json::Value oValue( Json::arrayValue );
					for ( const auto iter : tmp_peers) {
						if( bTerminating == true) {
							break;
						}

						oSlabPeerNames.push_back( iter.first );
						const auto & oSlabData = iter.second;

						Json::Value oSlabJson( Json::objectValue );
						oSlabJson[ "name" ] = iter.first;

						oSlabJson["block_size"] = oSlabData->GetBlockSize();
						oSlabJson["num_files"] = oSlabData->GetNumFiles();

						oSlabJson["swap_read_blocks"] = oSlabData->GetSwapReadBlocks();
						oSlabJson["swap_write_blocks"] = oSlabData->GetSwapWriteBlocks();
						oSlabJson["swap_idle_blocks"] = oSlabData->GetSwapFreeBlocks();

						oSlabJson["mem_read_blocks"] = oSlabData->GetMemReadBlocks();
						oSlabJson["mem_write_blocks"] = oSlabData->GetMemWriteBlocks();
						oSlabJson["mem_idle_blocks"] = oSlabData->GetMemFreeBlocks();

						oSlabJson["network_delay_usec"] = oSlabData->GetNetworkDelayUsec();

						oValue.append( oSlabJson );
					}

					oRoot[ "nodes" ] = oValue;
				}

				{
					oRoot[ "num_files" ] = oMetaFiles_m.size();
				}

				oStatusContainer.add( "base", StringUtils::ToJsonString( oRoot ) );

//		LOGGER_TRACE( "#" << __LINE__ << ", MetaFileManager::TraceNodes, Peers: " << oSlabPeerNames.qsize() << ", " << oSlabPeers_m.size() );
				TraceNodes();
			});
}

/**
 * 解析节点资源情况，如果是新节点进行资源注册
 */
bool MetaFileManager::NodeStatusPost(std::string& rs_host, unsigned short int rs_port,
		const std::shared_ptr<stringbuffer> & input) {

	std::string key = rs_host + ":" + StringUtils::ToString(rs_port);

	std::shared_ptr<SlabPeerData> oSlabData = std::make_shared<SlabPeerData>(rs_host, rs_port);

	if (oSlabData->ParseStatus(input) == true) {

		if (oSlabPeers_m.exists(key) == 0) {
			SYSLOG_WARN(
					"#" << __LINE__ << ", MetaFileManager::NodeStatusPost: " << oSlabData->getKey() << ", BlockSize: " << oSlabData->GetBlockSize()
					//
					<< ", MemReadBlocks: " << oSlabData->GetMemReadBlocks( ) << ", MemWriteBlocks: " << oSlabData->GetMemWriteBlocks()
					//
					<< ", MemFreeBlocks: " << oSlabData->GetMemFreeBlocks() << ", SwapReadBlocks: " << oSlabData->GetSwapReadBlocks( )
					//
					<< ", SwapWriteBlocks: " << oSlabData->GetSwapWriteBlocks() << ", SwapFreeBlocks: " << oSlabData->GetSwapFreeBlocks()
					//
					<< ", NetworkDelay(usec): " << oSlabData->GetNetworkDelayUsec() << ", Nodes: " << oSlabPeers_m.size() + 1)
		}

		oSlabPeers_m.put(key, oSlabData);
		oSlabPeerNames.push_back(key);

		return true;
	}

	LOGGER_TRACE("#" << __LINE__ << ", MetaFileManager::NodeStatusPost: " << oSlabData->getKey() << ", Invalid Status");
	return false;
}

bool MetaFileManager::GetSlabPeer(const std::string& peer, uint64_t & ns_usec) {
	std::shared_ptr<SlabPeerData> oPeerData;
	oSlabPeers_m.get(peer, oPeerData);
	if (oPeerData.get() != NULL) {
		ns_usec = oPeerData->GetNetworkDelayUsec();
		return true;
	}
	return false;
}

std::string MetaFileManager::GetStatus(const std::string& key) {
	std::string value;
	if (oStatusContainer.get(key, value) == true) {
		return value;
	}

	return "{}";
}

void MetaFileManager::start() {
	TraceNodes();
	TraceTrushes(1.0);
}

void MetaFileManager::stop() {
	if (bTerminating) {
		return;
	}

	bTerminating = true;

	boost::system::error_code ec;
	timer_Nodes->cancel(ec);
	timer_Trush->cancel(ec);

	LOGGER_TRACE("#" << __LINE__ << ", MetaFileManager::stop, Terminating");
}
/**
 * 解析节点资源情况
 */
bool SlabPeerData::ParseStatus(const std::shared_ptr<stringbuffer> & input) {
	Update();

	uint64_t t = 0;
	if (input->read_uint64(t) == false) {
		return false;
	}
	network_delay_usec = t;

	if (input->read_uint32(block_size) == false
			|| //
			input->read_uint64(mem_read_blocks) == false || input->read_uint64(mem_write_blocks) == false
			|| input->read_uint64(mem_free_blocks) == false
			|| //
			input->read_uint64(swap_read_blocks) == false || input->read_uint64(swap_write_blocks) == false
			|| input->read_uint64(swap_free_blocks) == false) {
		return false;
	}

	if (input->read_uint64(num_files) == false) {
	}

	return true;
}

void StatusContainer::add(const std::string& key, const std::string& val) {
	oStatusData.put(key, val);
}

bool StatusContainer::get(const std::string & key, std::string & val) {
	return oStatusData.get(key, val);
}
