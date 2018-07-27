package com.bd.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hbase.common.Constants;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HbaseQueryTest {

	@Autowired
	private HBaseUtils hBaseUtils;

	private String tableName = "cust";

	@Test
	public void test() {// done
		// 准备数据见：data_cust.txt:scan 'cust'
		System.out.println("1.====================");
		Map<String, String> map1 = hBaseUtils.query.get(tableName, "liyuan");
		System.out.println(map1);
		System.out.println("2.====================");
		Map<String, String> map2 = hBaseUtils.query.getRow(tableName, "liyuan", "info");
		System.out.println(map2);
		System.out.println("3.====================");
		String val = hBaseUtils.query.getColumn(tableName, "liyuan", "info", "firstname");
		System.out.println(val);
		// {area=pudong, secondname=yuan, firstname=li, city=shanghai}
		// 2.====================
		// {secondname=yuan, firstname=li}
		// 3.====================
		// li
	}

	@Test
	public void test1() {// done
		// 准备数据见：data_cust.txt:scan 'cust'
		System.out.println("1.====================");
		Long value = hBaseUtils.query.getRangeColumnCount("member", "aobama", "info");
		System.out.println(value);

		System.out.println("2.====================");
		Map<String, String> params2 = new HashMap<String, String>();
		params2.put(Constants.params.startColumn, "age");
		params2.put(Constants.params.endColumn, "secondname");
		List<String> list = hBaseUtils.query.getRangeColumn("member", "aobama", "info", params2);
		System.out.println(list);
		System.out.println("3.====================");
		Map<String, String> params3 = new HashMap<String, String>();
		params3.put(Constants.params.endColumn, "secondname");
		List<String> list2 = hBaseUtils.query.getRangeColumn("member", "aobama", "info", params3);
		System.out.println(list2);
		System.out.println("4.====================");
		Map<String, String> params4 = new HashMap<String, String>();
		params4.put(Constants.params.startColumn, "a");
		params4.put(Constants.params.endColumn, "x");
		List<String> list3 = hBaseUtils.query.getRangeColumn("member", "aobama", "info", params4);
		System.out.println(list3);
		System.out.println("5.====================");
		Map<String, String> params5 = new HashMap<String, String>();
		params5.put(Constants.params.startColumn, "age");
		params5.put(Constants.params.endColumn, "age2");
		Long val = hBaseUtils.query.getMergerValue("member", "liyuan6", "info", params5);
		System.out.println(val);
		// 1.====================
		// 4
		// 2.====================
		// [age:18,firstname:bama,secondname:ao]
		// 3.====================
		// [age:18,firstname:bama,secondname:ao]
		// 4.====================
		// [age:18,firstname:bama,secondname:ao,sex:m]
		// 5.====================
		// 60
	}

	public static void main(String[] args) {
		List<String> l = new ArrayList<String>();
		l.add("ssss");
		l.add("eeee");
		System.out.println(l);
	}
	// hbase(main):022:0> scan 'member'
	// ROW COLUMN+CELL
	// aobama column=info:age, timestamp=1527215921070, value=18
	// aobama column=info:firstname, timestamp=1527215921070, value=bama
	// aobama column=info:secondname, timestamp=1527215921070, value=ao
	// aobama column=info:sex, timestamp=1527215921070, value=m
	// aobama1 column=addr:addr, timestamp=1527215921090, value=4th
	// aobama1 column=addr:city, timestamp=1527215921090, value=new york
	// aobama1 column=addr:country, timestamp=1527215921090, value=US
	// aobama1 column=info:age, timestamp=1527215921090, value=19
	// aobama1 column=info:firstname, timestamp=1527215921090, value=bama1
	// aobama1 column=info:secondname, timestamp=1527215921090, value=ao1
	// aobama1 column=info:sex, timestamp=1527215921090, value=m
	// liyuan column=addr:area, timestamp=1527215920917, value=pudong
	// liyuan column=addr:city, timestamp=1527215920903, value=shanghai
	// liyuan column=info:age, timestamp=1527216170812, value=20,20,20|
	// liyuan column=info:firstname, timestamp=1527216170812, value=lili1,li1,li1|
	// liyuan column=info:secondname, timestamp=1527216170812, value=yuanyuan1,yuan1,yuan1|
	// liyuan column=info:sex, timestamp=1527216170812, value=m1,m1,m1|
	// liyuan2 column=info:age, timestamp=1527217744925, value=18
	// liyuan2 column=info:firstname, timestamp=1527217744901, value=li2li3
	// liyuan2 column=info:secondname, timestamp=1527163814402, value=yuan2
	// liyuan3 column=info:age, timestamp=1527228033636, value=\x00\x00\x00\x00\x00\x00\x00\x15
	// liyuan4 column=info:age, timestamp=1527228033667, value=\x00\x00\x00\x00\x00\x00\x00\x0F
	// liyuan5 column=info:age, timestamp=1527228033681, value=\x00\x00\x00\x00\x00\x00\x00\x12
	// liyuan6 column=info:age, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
	// liyuan6 column=info:age1, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
	// liyuan6 column=info:age2, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
	// lkq column=addr:area, timestamp=1527215921051, value=tiananmen
	// lkq column=addr:city, timestamp=1527215921037, value=beijing
	// lkq column=info:age, timestamp=1527215921024, value=18
	// lkq column=info:firstname, timestamp=1527215920995, value=l
	// lkq column=info:sex, timestamp=1527215921010, value=m
	// xjp column=addr:area, timestamp=1527215920981, value=tiananmen
	// xjp column=addr:city, timestamp=1527215920966, value=beijing
	// xjp column=info:firstname, timestamp=1527215920931, value=x
	// xjp column=info:secondname, timestamp=1527215920952, value=jp
	// 10 row(s) in 0.0430 seconds
	//
	// hbase(main):023:0>

	// 查询数据
	// a）查询某行记录
	//
	// # 语法：get <table>,<rowkey>,[<family:column>,....]
	// # 例如：查询表t1，rowkey001中的f1下的col1的值
	// hbase(main)> get 't1','rowkey001', 'f1:col1'
	// # 或者：
	// hbase(main)> get 't1','rowkey001', {COLUMN=>'f1:col1'}
	// # 查询表t1，rowke002中的f1下的所有列值
	// hbase(main)> get 't1','rowkey001'
	// b）扫描表
	//
	// # 语法：scan <table>, {COLUMNS => [ <family:column>,.... ], LIMIT => num}
	// # 另外，还可以添加STARTROW、TIMERANGE和FITLER等高级功能
	// # 例如：扫描表t1的前5条数据
	// hbase(main)> scan 't1',{LIMIT=>5}
	// c）查询表中的数据行数
	//
	// # 语法：count <table>, {INTERVAL => intervalNum, CACHE => cacheNum}
	// # INTERVAL设置多少行显示一次及对应的rowkey，默认1000；CACHE每次去取的缓存区大小，默认是10，调整该参数可提高查询速度
	// # 例如，查询表t1中的行数，每100条显示一次，缓存区为500
	// hbase(main)> count 't1', {INTERVAL => 100, CACHE => 500}
}
