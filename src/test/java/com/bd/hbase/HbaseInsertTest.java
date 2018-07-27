package com.bd.hbase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HbaseInsertTest {

	@Autowired
	private HBaseUtils hBaseUtils;

	@Test
	public void test() {// done
		// put 'yuan', 'rk1', 'f1:col1', 'val1'
		// scan 'yuan'
		// insert时，必须指定存在的表，列簇
		hBaseUtils.insert.putColumn("yuan", "rk1", "f1", "col1", "val1");
		// 抛错，因为没有f11，tableName，family必须已经建立
		hBaseUtils.insert.putColumn("yuan", "rk2", "f11", "col1", "val1");

		// hbase(main):025:0> scan 'yuan'
		// ROW COLUMN+CELL
		// rk1 column=f1:col1, timestamp=1527215643010, value=val2
		// 1 row(s) in 0.0170 seconds
	}

	@Test
	public void test1() {// done
		hBaseUtils.insert.putColumn("member", "liyuan", "info", "firstname", "li");
		hBaseUtils.insert.putColumn("member", "liyuan", "info", "secondname", "yuan");
		hBaseUtils.insert.putColumn("member", "liyuan", "addr", "city", "shanghai");
		hBaseUtils.insert.putColumn("member", "liyuan", "addr", "area", "pudong");

		hBaseUtils.insert.putColumn("member", "xjp", "info", "firstname", "x");
		hBaseUtils.insert.putColumn("member", "xjp", "info", "secondname", "jp");
		hBaseUtils.insert.putColumn("member", "xjp", "addr", "city", "beijing");
		hBaseUtils.insert.putColumn("member", "xjp", "addr", "area", "tiananmen");

		hBaseUtils.insert.putColumn("member", "lkq", "info", "firstname", "l");
		hBaseUtils.insert.putColumn("member", "lkq", "info", "sex", "m");
		hBaseUtils.insert.putColumn("member", "lkq", "info", "age", "18");
		hBaseUtils.insert.putColumn("member", "lkq", "addr", "city", "beijing");
		hBaseUtils.insert.putColumn("member", "lkq", "addr", "area", "tiananmen");

		// hBaseUtils.admin.createTable("member", "info", "addr");

		// [hadoop@auth-sit bigdata]$ hbase shell

		// put 'member', 'liyuan2', 'info:firstname', 'li2'
		// put 'member', 'liyuan2', 'info:secondname', 'yuan2'

		// hbase(main):002:0> describe 'member'
		// Table member is ENABLED
		// member
		// COLUMN FAMILIES DESCRIPTION
		// {NAME => 'addr', BLOOMFILTER => 'ROWCOL', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE',
		// DATA_BLOCK_ENCODING => 'DIFF', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS
		// => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
		// {NAME => 'info', BLOOMFILTER => 'ROWCOL', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE',
		// DATA_BLOCK_ENCODING => 'DIFF', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS
		// => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
		// 2 row(s) in 0.2030 seconds
		//
		// hbase(main):003:0> scan 'member'
		// ROW COLUMN+CELL
		// liyuan column=addr:area, timestamp=1527163589366, value=pudong
		// liyuan column=addr:city, timestamp=1527163589313, value=shanghai
		// liyuan column=info:firstname, timestamp=1527163589201, value=li
		// liyuan column=info:secondname, timestamp=1527163589261, value=yuan
		// liyuan2 column=info:firstname, timestamp=1527163773095, value=li2
		// liyuan2 column=info:secondname, timestamp=1527163814402, value=yuan2
		// lkq column=addr:area, timestamp=1527163589852, value=tiananmen
		// lkq column=addr:city, timestamp=1527163589810, value=beijing
		// lkq column=info:age, timestamp=1527163589775, value=18
		// lkq column=info:firstname, timestamp=1527163589683, value=l
		// lkq column=info:sex, timestamp=1527163589735, value=m
		// xjp column=addr:area, timestamp=1527163589627, value=tiananmen
		// xjp column=addr:city, timestamp=1527163589556, value=beijing
		// xjp column=info:firstname, timestamp=1527163589425, value=x
		// xjp column=info:secondname, timestamp=1527163589491, value=jp
		// 4 row(s) in 0.0780 seconds
		//
		// hbase(main):004:0>

	}

	@Test
	public void test2() {// done
		Map<String, String> map = new HashMap<String, String>();
		map.put("firstname", "bama");
		map.put("secondname", "ao");
		map.put("sex", "m");
		map.put("age", "18");

		hBaseUtils.insert.putColumn("member", "aobama", "info", map);

		// aobama column=info:age, timestamp=1527213803128, value=18
		// aobama column=info:firstname, timestamp=1527213803128, value=bama
		// aobama column=info:secondname, timestamp=1527213803128, value=ao
		// aobama column=info:sex, timestamp=1527213803128, value=m
	}

	@Test
	public void test3() {// done
		Map<String, Map<String, String>> maps = new HashMap<String, Map<String, String>>();

		Map<String, String> map1 = new HashMap<String, String>();
		map1.put("firstname", "bama1");
		map1.put("secondname", "ao1");
		map1.put("sex", "m");
		map1.put("age", "19");
		maps.put("info", map1);

		Map<String, String> map2 = new HashMap<String, String>();
		map2.put("country", "US");
		map2.put("city", "new york");
		map2.put("addr", "4th");
		maps.put("addr", map2);

		hBaseUtils.insert.putColumn("member", "aobama1", maps);

		// aobama1 column=addr:addr, timestamp=1527214546505, value=4th
		// aobama1 column=addr:city, timestamp=1527214546505, value=new york
		// aobama1 column=addr:country, timestamp=1527214546505, value=US
		// aobama1 column=info:age, timestamp=1527214546505, value=19
		// aobama1 column=info:firstname, timestamp=1527214546505, value=bama1
		// aobama1 column=info:secondname, timestamp=1527214546505, value=ao1
		// aobama1 column=info:sex, timestamp=1527214546505, value=m
	}

	// 添加数据
	// # 语法：put <table>,<rowkey>,<family:column>,<value>,<timestamp>
	// # 例如：给表t1的添加一行记录：rowkey是rowkey001，family name：f1，column name：col1，value：value01，timestamp：系统默认
	// hbase(main)> put 't1','rowkey001','f1:col1','value01'
}
