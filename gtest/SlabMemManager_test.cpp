#include <gtest/gtest.h>

#include "../memory/SlabMemManager.hpp"
#include "../memory/SlabMMapManager.hpp"

TEST(SlabMem, SlabMMapManager) {

	const std::string str = "This is a test string!";
	std::shared_ptr<SlabMemFactory> osmf(new SlabMemFactory(1));

	const std::string root = "/opt";
	std::shared_ptr<SlabFactory> smmm(new SlabMMapFactory(osmf, root, 2));

	{
		std::vector<std::shared_ptr<SlabBlock> > oSlabBlocks;
		for (int i = 0; i < NUMBLOCKS - 1; i++) {
			std::stringstream ss;
			ss << i << ": " << str;
			std::string tmp = ss.str();

			auto oSlabBlock = smmm->New();

			EXPECT_TRUE(oSlabBlock.get() != NULL);
			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());

			{
				std::string data = StringUtils::ToString(i);
				ASSERT_EQ(oSlabBlock->Write(8, 0, data), data.size());
			}

			ASSERT_EQ(oSlabBlock->GetVersion(), 2);

			{
				std::string data = StringUtils::ToString(i);
				ASSERT_EQ(oSlabBlock->Write(20, 0, data), data.size());
			}
			ASSERT_EQ(oSlabBlock->GetVersion(), 3);

			oSlabBlock->Commit();
			oSlabBlocks.push_back(oSlabBlock);
		}
		LOGGER_TRACE(
				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
		LOGGER_TRACE(
				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());

		{
			std::stringstream ss;
			ss << 5000 << ": " << str;
			std::string tmp = ss.str();

			auto oSlabBlock = smmm->New();

			EXPECT_TRUE(oSlabBlock.get() != NULL);
			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());

			oSlabBlock->Commit();
			oSlabBlocks.push_back(oSlabBlock);

			oSlabBlock->SetEditable(true);
			ASSERT_EQ(oSlabBlock->IsEditable(), true);
		}
		LOGGER_TRACE(
				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
		LOGGER_TRACE(
				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());

		for (int i = NUMBLOCKS; i < 2 * NUMBLOCKS; i++) {
			std::stringstream ss;
			ss << i << ": " << str;
			std::string tmp = ss.str();

			auto oSlabBlock = smmm->New();

			EXPECT_TRUE(oSlabBlock.get() != NULL);
			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());

			{
				std::string data = StringUtils::ToString(i);
				ASSERT_EQ(oSlabBlock->Write(8, 0, data), data.size());
			}

			ASSERT_EQ(oSlabBlock->GetVersion(), 2);

			{
				std::string data = StringUtils::ToString(i);
				ASSERT_EQ(oSlabBlock->Write(20, 0, data), data.size());
			}
			ASSERT_EQ(oSlabBlock->GetVersion(), 3);

			oSlabBlock->Commit();
			oSlabBlocks.push_back(oSlabBlock);
		}
		LOGGER_TRACE(
				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
		LOGGER_TRACE(
				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());

		{
			std::stringstream ss;
			ss << 10000 << ": " << str;
			std::string tmp = ss.str();

			auto oSlabBlock = smmm->New();

			EXPECT_TRUE(oSlabBlock.get() != NULL);
			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());

			oSlabBlock->Commit();
			oSlabBlocks.push_back(oSlabBlock);

			oSlabBlock->SetEditable(true);
			ASSERT_EQ(oSlabBlock->IsEditable(), true);
		}
		LOGGER_TRACE(
				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
		LOGGER_TRACE(
				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());

		for (std::shared_ptr<SlabBlock> oSlabBlock : oSlabBlocks) {
			LOGGER_TRACE(oSlabBlock->GetSlabId() << "-" << oSlabBlock->GetBlockId() << ":" << oSlabBlock->GetUsedSize());

			if (oSlabBlock->GetVersion() > 0) {
				size_t data_size = oSlabBlock->GetUsedSize();
				std::string data;
				ASSERT_EQ(oSlabBlock->Read(data_size, 0, data), data_size);
				std::cout << data << std::endl;
			} else {
				std::cout << "gcit:" << oSlabBlock->GetSlabId() << ":" << oSlabBlock->GetBlockId() << std::endl;
			}

			oSlabBlock->Free();
//			ASSERT_EQ(smmm->GetNumBlocks(), smmm->GetNumFreeBlocks() + smmm->GetNumUsedBlocks());
		}

		LOGGER_TRACE(
				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
		LOGGER_TRACE(
				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());

	}
}

//TEST(SlabMem, SlabMemManager) {
//
//	const std::string str = "This is a test string!";
//
//	std::shared_ptr<SlabMemFactory> smmm(new SlabMemFactory(1));
//
//	{
//		std::vector<std::shared_ptr<SlabBlock> > oSlabBlocks;
//		for (int i = 0; i < NUMBLOCKS - 1; i++) {
//			std::stringstream ss;
//			ss << i << ": " << str;
//			std::string tmp = ss.str();
//
//			auto oSlabBlock = smmm->New();
//
//			EXPECT_TRUE(oSlabBlock.get() != NULL);
//			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());
//
//			{
//				std::string data = StringUtils::ToString(i);
//				ASSERT_EQ(oSlabBlock->Write(8, 0, data), data.size());
//			}
//
//			ASSERT_EQ(oSlabBlock->GetVersion(), 2);
//
//			{
//				std::string data = StringUtils::ToString(i);
//				ASSERT_EQ(oSlabBlock->Write(20, 0, data), data.size());
//			}
//			ASSERT_EQ(oSlabBlock->GetVersion(), 3);
//
//			oSlabBlock->Commit();
//			oSlabBlocks.push_back(oSlabBlock);
//		}
//		LOGGER_TRACE(
//				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
//		LOGGER_TRACE(
//				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());
//
//		{
//			std::stringstream ss;
//			ss << 5000 << ": " << str;
//			std::string tmp = ss.str();
//
//			auto oSlabBlock = smmm->New();
//
//			EXPECT_TRUE(oSlabBlock.get() != NULL);
//			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());
//
//			oSlabBlock->Commit();
//			oSlabBlocks.push_back(oSlabBlock);
//
//			oSlabBlock->SetEditable(true);
//			ASSERT_EQ(oSlabBlock->IsEditable(), true);
//		}
//		LOGGER_TRACE(
//				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
//		LOGGER_TRACE(
//				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());
//
//		for (int i = NUMBLOCKS; i < 2 * NUMBLOCKS; i++) {
//			std::stringstream ss;
//			ss << i << ": " << str;
//			std::string tmp = ss.str();
//
//			auto oSlabBlock = smmm->New();
//
//			EXPECT_TRUE(oSlabBlock.get() != NULL);
//			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());
//
//			{
//				std::string data = StringUtils::ToString(i);
//				ASSERT_EQ(oSlabBlock->Write(8, 0, data), data.size());
//			}
//
//			ASSERT_EQ(oSlabBlock->GetVersion(), 2);
//
//			{
//				std::string data = StringUtils::ToString(i);
//				ASSERT_EQ(oSlabBlock->Write(20, 0, data), data.size());
//			}
//			ASSERT_EQ(oSlabBlock->GetVersion(), 3);
//
//			oSlabBlock->Commit();
//			oSlabBlocks.push_back(oSlabBlock);
//		}
//		LOGGER_TRACE(
//				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
//		LOGGER_TRACE(
//				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());
//
//		{
//			std::stringstream ss;
//			ss << 10000 << ": " << str;
//			std::string tmp = ss.str();
//
//			auto oSlabBlock = smmm->New();
//
//			EXPECT_TRUE(oSlabBlock.get() != NULL);
//			ASSERT_EQ(oSlabBlock->WriteBlock(tmp.c_str(), tmp.size(), 0), tmp.size());
//
//			oSlabBlock->Commit();
//			oSlabBlocks.push_back(oSlabBlock);
//
//			oSlabBlock->SetEditable(true);
//			ASSERT_EQ(oSlabBlock->IsEditable(), true);
//		}
//		LOGGER_TRACE(
//				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
//		LOGGER_TRACE(
//				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());
//
//		for (std::shared_ptr<SlabBlock> oSlabBlock : oSlabBlocks) {
//			LOGGER_TRACE(oSlabBlock->GetSlabId() << "-" << oSlabBlock->GetBlockId() << ":" << oSlabBlock->GetUsedSize());
//
//			if (oSlabBlock->GetVersion() > 0) {
//				size_t data_size = oSlabBlock->GetUsedSize();
//				std::string data;
//				ASSERT_EQ(oSlabBlock->Read(data_size, 0, data), data_size);
//				std::cout << data << std::endl;
//			} else {
//				std::cout << "gcit:" << oSlabBlock->GetSlabId() << ":" << oSlabBlock->GetBlockId() << std::endl;
//			}
//
//			oSlabBlock->Free();
////			ASSERT_EQ(smmm->GetNumBlocks(), smmm->GetNumFreeBlocks() + smmm->GetNumUsedBlocks());
//		}
//
//		LOGGER_TRACE(
//				"FreeMemBlocks: "<< smmm->GetFreeMemBlocks() << ", ReadMemBlocks: " << smmm->GetReadMemBlocks() << ", WriteMemBlocks: " << smmm->GetWriteMemBlocks());
//		LOGGER_TRACE(
//				"FreeSwapBlocks: "<< smmm->GetFreeSwapBlocks() << ", ReadSwapBlocks: " << smmm->GetReadSwapBlocks() << ", WriteSwapBlocks: " << smmm->GetWriteSwapBlocks());
//
//	}
//
//}
