package com.bd.hbase;

import java.util.HashMap;
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
public class HbaseQueryTest2 {

	@Autowired
	private HBaseUtils hBaseUtils;

	private String tableName = "cust";

	@Test
	public void test() {// done
		// 准备数据见：data_cust.txt:scan 'cust'
		System.out.println("1.====================");
		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.params.startRow, "liyuan");
		params.put(Constants.params.stopRow, "liyuan2");
		Map<String, Map<String, String>> map = hBaseUtils.query2.query(
				tableName, params);
		System.out.println(map);
		System.out.println("2.====================");
		Map<String, String> params2 = new HashMap<String, String>();
		params2.put(Constants.params.family, "info");
		params2.put(Constants.params.startRow, "liyuan");
		params2.put(Constants.params.stopRow, "liyuan2");
		Map<String, Map<String, String>> map2 = hBaseUtils.query2.query(
				tableName, params2);
		System.out.println(map2);
		System.out.println("3.====================");
		Map<String, String> params3 = new HashMap<String, String>();
		params3.put(Constants.params.family, "addr");
		params3.put("city", "city");
		params3.put(Constants.params.startRow, "liyuan");
		params3.put(Constants.params.stopRow, "liyuan2");
		Map<String, Map<String, String>> map3 = hBaseUtils.query2.query(
				tableName, params3);
		System.out.println(map3);
		System.out.println("4.====================");
		Map<String, String> params4 = new HashMap<String, String>();
		params4.put(Constants.params.family, "addr");
		params4.put(Constants.params.minStamp, "1527644514584");
		params4.put(Constants.params.maxStamp, "1527645089792");
		Map<String, Map<String, String>> map4 = hBaseUtils.query2.query(
				tableName, params4);
		System.out.println(map4);

		System.out.println("5.====================");
		Map<String, String> params5 = new HashMap<String, String>();
		params5.put(Constants.params.family, "addr");
		params5.put(Constants.params.startRow, "liyuan2");
		params5.put(Constants.params.limit, "1");
		Map<String, Map<String, String>> map5 = hBaseUtils.query2.query(
				tableName, params5);
		System.out.println(map5);
		// 1.====================
		// {liyuan1={addr:area=pudong, info:firstname=li, info:secondname=yuan,
		// addr:city=shanghai}, liyuan={addr:area=pudong,
		// info:firstname=li, info:secondname=yuan, addr:city=shanghai}}
		// 2.====================
		// {liyuan1={info:firstname=li, info:secondname=yuan},
		// liyuan={info:firstname=li, info:secondname=yuan}}
		// 3.====================
		// {liyuan1={addr:city=shanghai}, liyuan={addr:city=shanghai}}
		// 4.====================
		// {lkq={addr:area=tiananmen, addr:city=beijing},
		// liyuan={addr:area=pudong}, xjp={addr:area=tiananmen,
		// addr:city=beijing}}
	}

	@Test
	public void test2() {
		// 准备数据见：data_cust.txt:scan 'cust'

		System.out.println("1.====================");
		Map<String, Map<String, String>> map = hBaseUtils.query2.page(
				tableName, null, null, 1);
		System.out.println(map);
		System.out.println("2.====================");
		Map<String, Map<String, String>> map2 = hBaseUtils.query2.page(
				tableName, "xjp2", null, 10);
		System.out.println(map2);
		System.out.println("3.====================");
		Map<String, Map<String, String>> map3 = hBaseUtils.query2.page(
				tableName, "xjp", "xjp2", 10);
		System.out.println(map3);
		// 1.====================
		// {liyuan={addr:area=pudong, info:firstname=li, info:secondname=yuan,
		// addr:city=shanghai}}
		// 2.====================
		// {xjp2={addr:area=tiananmen, info:firstname=x, info:secondname=jp,
		// addr:city=beijing}}
		// 3.====================
		// {xjp1={addr:area=tiananmen, info:firstname=x, info:secondname=jp,
		// addr:city=beijing}, xjp={addr:area=tiananmen, info:firstname=x,
		// info:secondname=jp, addr:city=beijing}}

	}
}
