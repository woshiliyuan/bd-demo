package com.bd.hbase;

import org.junit.Test;

/**
 * @author yuan.li
 * 
 *         设计分区时按照rowkey的前缀
 */
public class TestSplit {
	@Test
	public void test1_1() {
		// hbase(main):043:0> create 'member12',{NAME => 'info', VERSIONS => 1},{SPLITS => ['100', '200', 'a', 'h']}
		// 0 row(s) in 1.2460 seconds
		//
		// => Hbase::Table - member12
		// hbase(main):044:0> describe 'member12'
		// Table member12 is ENABLED
		// member12
		// COLUMN FAMILIES DESCRIPTION
		// {NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING
		// => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS =>
		// '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
		// 1 row(s) in 0.0340 seconds
		//
		// hbase(main):045:0>

		// http://auth-sit:60030/rs-status?filter=general#regionBaseInfo
		//
		// Region Name Start Key End Key ReplicaID
		// hbase:meta,,1.1588230740 0
		// member11,,1529475738770.acdaad79d2e92210bc9a4f87f8711b17. 0
		// member12,,1529636545964.95e73d65a98aee2e6831fe9dc2d3755a. 100 0
		// member12,100,1529636545964.01ef8bd406405fab5965d1383adecfa4. 100 200 0
		// member12,200,1529636545964.de9e0621129d53c6d730139b7cdefaeb. 200 a 0
		// member12,a,1529636545964.cb9bc8170d22255c24bba1a6947e2766. a h 0
		// member12,h,1529636545964.80012118866677406a5fc21ce55f2785. h 0
		// cust,,1527644155069.d71d3fd19a816eec5f08f521695c7190. 0
		// cust1,,1529578142994.111be3e0580bc766b299e72d58b731e1. 0
		// hbase:namespace,,1527576105448.301decbda15cf5382717d700be1a5662. 0
		// member1,,1529390500079.4b36edf2a6c1ca583b7121e63e2aa443. 0
	}

	@Test
	public void test1_2() {

		// put 'member12', '100abc', 'info:firstname', 'xjp'
		// put 'member12', '110abc', 'info:firstname', 'xjp'
		// put 'member12', '200abc', 'info:firstname', 'xjp'
		// put 'member12', '210abc', 'info:firstname', 'xjp'
		// put 'member12', '9', 'info:firstname', 'xjp'
		// put 'member12', '999', 'info:firstname', 'xjp'

		// put 'member12', 'abc', 'info:firstname', 'xjp'
		// put 'member12', 'dbc', 'info:firstname', 'xjp'
		// put 'member12', 'hbc', 'info:firstname', 'xjp'
		// put 'member12', 'xyz', 'info:firstname', 'xjp'

		// flush 'member12'
	}

	@Test
	public void test1_3() {
		// hbase(main):006:0> scan 'member12'
		// ROW COLUMN+CELL
		// 100abc column=info:firstname, timestamp=1529636841851, value=xjp
		// 110abc column=info:firstname, timestamp=1529636905551, value=xjp
		// 200abc column=info:firstname, timestamp=1529636911155, value=xjp
		// 210abc column=info:firstname, timestamp=1529636916317, value=xjp
		// 9 column=info:firstname, timestamp=1529636920910, value=xjp
		// 999 column=info:firstname, timestamp=1529636925630, value=xjp
		// abc column=info:firstname, timestamp=1529636932101, value=xjp
		// dbc column=info:firstname, timestamp=1529636937446, value=xjp
		// hbc column=info:firstname, timestamp=1529636942362, value=xjp
		// xyz column=info:firstname, timestamp=1529636946722, value=xjp
		// 10 row(s) in 0.1020 seconds
		//
		// hbase(main):007:0>

		// 可知，参见http://auth-sit:60030/rs-status?filter=general#regionStoreStats
		//
		// 1），100abc到110abc保存在同一个分区
		// count = 2
		// Key of biggest row: 100abc
		//
		// 2），200abc到999保存在同一个分区
		// count = 4
		// Key of biggest row: 200abc
		//
		// 3），abc到dbc保存在同一个分区
		// count = 2
		// Key of biggest row: abc
		//
		// 4）， hbc到xyz保存在同一个分区
		// count = 2
		// Key of biggest row: hbc

	}
}
