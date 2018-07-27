package com.bd.hbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hbase.common.Constants;
import com.bd.hbase.common.QualifierModel;

/**
 * @author yuan.li
 *
 *
 *         VERSIONS：指版本数
 * 
 *         MIN_VERSIONS=> '0'：最小版本数
 * 
 *         TTL=> 'FOREVER'：版本存活时间(秒)
 * 
 *         假设versions=10,mini_version=4
 * 
 *         到达TTL时间后，version-mini_version=6，最老的6个版本的值会被清空
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class TestVersions {

	@Autowired
	private HBaseUtils hBaseUtils;

	/**
	 * VERSIONS
	 * 
	 * create..VERSIONS:表-row key-列簇:列，维度保存的个数，默认为最新的一个，当记录大于VERSIONS个数时，会移除最旧的数据
	 * 
	 * get..VERSIONS:一次获取多少个版本的数据，默认为最新的一个
	 */
	@Test
	public void test1_1() {
		// create 'member1', {NAME => 'info', VERSIONS => '3'}
		// hbase(main):011:0> describe 'member1'
		// Table member1 is ENABLED
		// member1
		// COLUMN FAMILIES DESCRIPTION
		// {NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '3', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING
		// => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS =>
		// '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
		// 1 row(s) in 0.0420 seconds

		// put 'member1', 'xjp', 'info:firstname', 'x1'
		// put 'member1', 'xjp', 'info:firstname', 'x2'
		// put 'member1', 'xjp', 'info:firstname', 'x3'
		// put 'member1', 'xjp', 'info:firstname', 'x4'
		// put 'member1', 'xjp', 'info:secondname', 'jp1'
		// put 'member1', 'xjp', 'info:secondname', 'jp2'
		// put 'member1', 'xjp', 'info:secondname', 'jp3'
		// put 'member1', 'xjp', 'info:secondname', 'jp4'

		// hbase(main):001:0> get 'member1','xjp'
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:secondname timestamp=1529393810186, value=jp4
		// 2 row(s) in 0.3700 seconds
		//
		// hbase(main):002:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>5}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:firstname timestamp=1529393011312, value=x3
		// info:firstname timestamp=1529393007012, value=x2
		// info:secondname timestamp=1529393810186, value=jp4
		// info:secondname timestamp=1529393024980, value=jp3
		// info:secondname timestamp=1529393022476, value=jp2
		// 6 row(s) in 0.0400 seconds
		//
		// hbase(main):003:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>4}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:firstname timestamp=1529393011312, value=x3
		// info:firstname timestamp=1529393007012, value=x2
		// info:secondname timestamp=1529393810186, value=jp4
		// info:secondname timestamp=1529393024980, value=jp3
		// info:secondname timestamp=1529393022476, value=jp2
		// 6 row(s) in 0.0250 seconds
		//
		// hbase(main):004:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>3}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:firstname timestamp=1529393011312, value=x3
		// info:firstname timestamp=1529393007012, value=x2
		// info:secondname timestamp=1529393810186, value=jp4
		// info:secondname timestamp=1529393024980, value=jp3
		// info:secondname timestamp=1529393022476, value=jp2
		// 6 row(s) in 0.0280 seconds
		//
		// hbase(main):005:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>2}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:firstname timestamp=1529393011312, value=x3
		// info:secondname timestamp=1529393810186, value=jp4
		// info:secondname timestamp=1529393024980, value=jp3
		// 4 row(s) in 0.0440 seconds
		//
		// hbase(main):006:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>1}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:secondname timestamp=1529393810186, value=jp4
		// 2 row(s) in 0.0210 seconds
		//
		// hbase(main):007:0>

		// hbase(main):010:0> put 'member1', 'xjp', 'info:secondname', 'jp4'
		// hbase(main):012:0> get 'member1','xjp',{COLUMN=>'info',VERSIONS=>3}
		// COLUMN CELL
		// info:firstname timestamp=1529393742584, value=x4
		// info:firstname timestamp=1529393011312, value=x3
		// info:firstname timestamp=1529393007012, value=x2
		// info:secondname timestamp=1529638611633, value=jp4
		// info:secondname timestamp=1529393810186, value=jp4
		// info:secondname timestamp=1529393024980, value=jp3
		// 6 row(s) in 0.0200 seconds
		//
		// hbase(main):013:0>
	}

	/**
	 * timestamp
	 */
	@Test
	public void test1_2() {
		// put <table>,<rowkey>,<family:column>,<value>,<timestamp>
		//
		// put 'member1', 'lkq', 'info:firstname', 'l1',1
		// put 'member1', 'lkq', 'info:firstname', 'l2',2
		// put 'member1', 'lkq', 'info:firstname', 'l3',3
		// put 'member1', 'lkq', 'info:firstname', 'l4',4

		// hbase(main):016:0> get 'member1','lkq',{COLUMN=>'info',VERSIONS=>3}
		// COLUMN CELL
		// info:firstname timestamp=4, value=l4
		// info:firstname timestamp=3, value=l3
		// info:firstname timestamp=2, value=l2
		// 3 row(s) in 0.0200 seconds
		//
		// hbase(main):017:0>

		// hbase(main):017:0> get 'member1', 'lkq', {COLUMN=>'info',TIMESTAMP => 2}
		// COLUMN CELL
		// info:firstname timestamp=2, value=l2
		// 1 row(s) in 0.0290 seconds
		//
		// hbase(main):018:0>

		// hbase(main):023:0> get 'member1', 'lkq', {COLUMN=>'info',TIMERANGE=>[2,4],VERSIONS=>3}
		// COLUMN CELL
		// info:firstname timestamp=3, value=l3
		// info:firstname timestamp=2, value=l2
		// 2 row(s) in 0.0060 seconds
		//
		// hbase(main):024:0>
	}

	/**
	 * java api
	 */
	@Test
	public void test1_3() {
		System.out.println("1.====================");
		Map<String, List<QualifierModel>> map = hBaseUtils.query1.query("member1", "xjp", null, null);
		System.out.println(map);
		System.out.println("2.====================");
		Map<String, String> params2 = new HashMap<String, String>();
		params2.put(Constants.params.versions, "5");
		Map<String, List<QualifierModel>> map2 = hBaseUtils.query1.query("member1", "xjp", "info", params2);
		System.out.println(map2);
		System.out.println("3.====================");
		Map<String, String> params3 = new HashMap<String, String>();
		params3.put(Constants.params.versions, "3");
		Map<String, List<QualifierModel>> map3 = hBaseUtils.query1.query("member1", "xjp", "info", params3);
		System.out.println(map3);
		System.out.println("4.====================");
		Map<String, String> params4 = new HashMap<String, String>();
		params3.put(Constants.params.versions, "1");
		Map<String, List<QualifierModel>> map4 = hBaseUtils.query1.query("member1", "xjp", "info", params4);
		System.out.println(map4);
		// 1.====================
		// {info=[QualifierModel [qualifier=firstname, value=x4, timestamp=1529393742584], QualifierModel [qualifier=secondname, value=jp4,
		// timestamp=1529638611633]]}
		// 2.====================
		// {info=[QualifierModel [qualifier=firstname, value=x4, timestamp=1529393742584], QualifierModel [qualifier=firstname, value=x3,
		// timestamp=1529393011312], QualifierModel [qualifier=firstname, value=x2, timestamp=1529393007012], QualifierModel
		// [qualifier=secondname, value=jp4, timestamp=1529638611633], QualifierModel [qualifier=secondname, value=jp4,
		// timestamp=1529393810186], QualifierModel [qualifier=secondname, value=jp3, timestamp=1529393024980]]}
		// 3.====================
		// {info=[QualifierModel [qualifier=firstname, value=x4, timestamp=1529393742584], QualifierModel [qualifier=firstname, value=x3,
		// timestamp=1529393011312], QualifierModel [qualifier=firstname, value=x2, timestamp=1529393007012], QualifierModel
		// [qualifier=secondname, value=jp4, timestamp=1529638611633], QualifierModel [qualifier=secondname, value=jp4,
		// timestamp=1529393810186], QualifierModel [qualifier=secondname, value=jp3, timestamp=1529393024980]]}
		// 4.====================
		// {info=[QualifierModel [qualifier=firstname, value=x4, timestamp=1529393742584], QualifierModel [qualifier=secondname, value=jp4,
		// timestamp=1529638611633]]}
	}

	/**
	 * TTL
	 */
	@Test
	public void test2_1() {
		// create 'member11', {NAME => 'info', VERSIONS => '3',MIN_VERSIONS => '1',TTL =>'180'}

		// hbase(main):011:0> describe 'member11'
		// Table member11 is ENABLED
		// member11
		// COLUMN FAMILIES DESCRIPTION
		// {NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '3', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING
		// => 'NONE', TTL => '180 SECONDS (3 MINUTES)', COMPRESSION => 'NONE',
		// MIN_VERSIONS => '1', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
		// 1 row(s) in 0.0990 seconds
		//
		// hbase(main):012:0>

		// put 'member11', 'xjp', 'info:firstname', 'x1'
		// put 'member11', 'xjp', 'info:firstname', 'x2'
		// put 'member11', 'xjp', 'info:firstname', 'x3'
		// put 'member11', 'xjp', 'info:firstname', 'x4'
		// put 'member11', 'xjp', 'info:secondname', 'jp1'
		// put 'member11', 'xjp', 'info:secondname', 'jp2'
		// put 'member11', 'xjp', 'info:secondname', 'jp3'
		// put 'member11', 'xjp', 'info:secondname', 'jp4'

		// TTL时间后
		// hbase(main):023:0> get 'member11','xjp',{COLUMN=>'info',VERSIONS=>4}
		// COLUMN CELL
		// info:firstname timestamp=1529475828757, value=x4
		// info:secondname timestamp=1529475845980, value=jp4
		// 2 row(s) in 0.0050 seconds
		//
		// hbase(main):024:0>
	}
}
