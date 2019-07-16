/*
 * MetaFile.inc
 *
 *  Created on: Nov 23, 2018
 *      Author: root
 */

#include <databox/cpl_memdog.hpp>

INIT_IG(MetaFile_watchdog, "MetaFile");
INIT_IG(MetaBlock_watchdog, "MetaBlock");

MetaFile::MetaFile(const std::shared_ptr<MetaFileManager>& oMetaFileManager_, const std::string & filename_, int uuid_) :
		oMetaFileManager(oMetaFileManager_), filename(filename_), uuid(uuid_) {
	LOGGER_TRACE("#" << __LINE__ << ", MetaFile::MetaFile, " << filename_ << ", " << uuid_);
	INC_IG(MetaFile_watchdog);
}

MetaFile::~MetaFile() {
	LOGGER_TRACE("#" << __LINE__ << ", MetaFile::~MetaFile, " << filename << ", " << uuid);
	DEC_IG(MetaFile_watchdog);
}


std::shared_ptr<MetaBlock> MetaFile::GetOrCreate(uint32_t block_offset_id) {
	std::shared_ptr < MetaBlock > oMetaBlock;

	auto self = this->shared_from_this();
	oMetaBlocks.get_or_create(block_offset_id, oMetaBlock, [ this, self, block_offset_id ] () {
		return std::make_shared<MetaBlock>( );
	});

	if (oMetaBlock.get() != NULL) {
		oMetaBlock->Update();
	}

	return oMetaBlock;
}

std::shared_ptr<MetaBlock> MetaFile::Get(uint32_t block_offset_id) {
	std::shared_ptr < MetaBlock > oMetaBlock;
	oMetaBlocks.get(block_offset_id, oMetaBlock);

	if (oMetaBlock.get() != NULL) {
		oMetaBlock->Update();
	}
	return oMetaBlock;
}

void MetaFile::GetKeys(std::vector<uint32_t> & keys) {
	oMetaBlocks.get_keys(keys);
}

void MetaFile::RemovePeer(uint32_t block_offset_id, const std::string & peer) {
	std::shared_ptr < MetaBlock > oMetaBlock;
	oMetaBlocks.get(block_offset_id, oMetaBlock);

	if (oMetaBlock.get() == NULL) {
		return;
	}

	size_t peers = oMetaBlock->RemovePeer(peer);
	LOGGER_TRACE(
			"#" << __LINE__ << ", MetaFile::RemovePeer, " << this->filename << ", " << block_offset_id << " --> "
					<< peer << ", peers: " << peers);

	if (peers == 0) {
		oMetaBlocks.erase(block_offset_id);
	} else {
		oMetaBlock->Update();
	}
}

void MetaBlock::AddPeer(const std::shared_ptr<SlabPeer>& oSlabPeer) {
	const std::string & peer = oSlabPeer->getKey();
	if (oSlabPeers.exists(peer) == 0) {
		oSlabPeers.put(peer, oSlabPeer);
	}
}

size_t MetaBlock::RemovePeer(const std::string & peer) {
	oSlabPeers.erase(peer);
	return oSlabPeers.size();
}

size_t MetaBlock::RemovePeer(const std::shared_ptr<SlabPeer>& oSlabPeer) {
	return RemovePeer(oSlabPeer->getKey());
}

int MetaBlock::GetPeers(std::vector<std::string>& peers) {
	return oSlabPeers.get_keys(peers);
}

int MetaBlock::GetPeers(std::vector<std::string>& peers, const std::string & host, unsigned short int port) {
	std::vector < std::string > peers_t;
	std::string mykey = host + ":" + StringUtils::ToString(port);
	oSlabPeers.get_keys(peers_t);

	for (const std::string & peer : peers_t) {
		if (mykey != peer) {
			peers.push_back(peer);
		}
	}
	return peers.size();
}

void MetaBlock::ClearPeers() {
	oSlabPeers.clear();
}

MetaBlock::MetaBlock() {
	INC_IG(MetaBlock_watchdog);
	Update();
}

MetaBlock::~MetaBlock() {
	DEC_IG(MetaBlock_watchdog);
}
