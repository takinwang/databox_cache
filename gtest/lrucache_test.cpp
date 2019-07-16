/*
 * lrucache_test.cpp
 *
 *  Created on: Dec 23, 2018
 *      Author: root
 */

//#include <gtest/gtest.h>
#include <databox/lrucache.hpp>

class TestData {

};

int main() {

	cache::lru_cache_count_num<int, std::shared_ptr<TestData> > test_lst(100);

	for (int i = 0; i < 100; i++) {
		std::shared_ptr<TestData> d(new TestData);
		test_lst.put(i, d, 10);
	}

	std::shared_ptr<TestData> d;
	test_lst.get_or_create(11, d, 10, []( ) {
		return std::make_shared<TestData> ( );
	});

	cache::lru_cache_data<std::shared_ptr<TestData> > *pvalue;
	test_lst.get(12, &pvalue);

	for (int i = 50; i < 100; i++) {
		std::shared_ptr<TestData> d(new TestData);
		test_lst.put(i, d, 10);
	}

	test_lst.exists(23);

	test_lst.erase_nss(12);

	test_lst.erase(10);

	test_lst.pop(1, d);

	test_lst.gc();

	test_lst.clear();
}
