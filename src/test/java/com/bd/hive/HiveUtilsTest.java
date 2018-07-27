package com.bd.hive;

import java.util.List;
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
public class HiveUtilsTest {
	@Autowired
	private HiveUtils hiveUtils;

	@Test
	public void test() {
		// hive (default)> create database db_test;
		// hive (default)> use db_test;
		// hive (db_test)> create table employees (id int comment 'id',name string comment 'name',country string comment 'country',state
		// string
		// comment 'state')row format delimited fields terminated by ',';
		// OK
		// Time taken: 0.293 seconds
		// hive (db_test)> describe employees;
		// OK
		// col_name data_type comment
		// id int id
		// name string name
		// country string country
		// state string state
		// Time taken: 0.424 seconds, Fetched: 4 row(s)
		// hive (db_test)>
		String sql = "describe employees";
		hiveUtils.execute(sql);

	}

	@Test
	public void test1() {
		// hive (db_test)> insert into employees(id,name,country,state)values(0,'xjp','china','bj');
		// hive (db_test)> select * from employees;
		// Warning: fs.defaultFS is not set when running "chgrp" command.
		// Warning: fs.defaultFS is not set when running "chmod" command.
		// OK
		// employees.id employees.name employees.country employees.state
		// 0 xjp china bj
		// Time taken: 0.135 seconds, Fetched: 1 row(s)
		// hive (db_test)>

		String sql = "select * from employees";
		List<Map<String, Object>> list = hiveUtils.executeQuery(sql);
		for (Map<String, Object> map : list) {
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				System.out.printf(entry.getKey() + ":" + entry.getValue() + "\t");
			}
			System.out.println();
		}
		// employees.name:xjp employees.state:bj employees.country:china employees.id:0

	}

	@Test
	public void test2() {
		// hive (db_test)> load data local inpath '/apps/svr/bigdata/hive/test.txt' overwrite into table employees;
		// Loading data to table db_test.employees
		// Warning: fs.defaultFS is not set when running "chgrp" command.
		// Warning: fs.defaultFS is not set when running "chmod" command.
		// Table db_test.employees stats: [numFiles=1, numRows=0, totalSize=73, rawDataSize=0]
		// OK
		// Time taken: 0.875 seconds
		// hive (db_test)> select * from employees;
		// Warning: fs.defaultFS is not set when running "chgrp" command.
		// Warning: fs.defaultFS is not set when running "chmod" command.
		// OK
		// employees.id employees.name employees.country employees.state
		// 1 'xjp1' 'china1' 'bj1'
		// 2 'xjp2' 'china2' 'bj2'
		// 3 'xjp3' 'china4' 'bj3'
		// Time taken: 0.12 seconds, Fetched: 3 row(s)
		// hive (db_test)> load data local inpath '/apps/svr/bigdata/hive/test.txt' into table employees;
		// Loading data to table db_test.employees
		// Warning: fs.defaultFS is not set when running "chgrp" command.
		// Warning: fs.defaultFS is not set when running "chmod" command.
		// Table db_test.employees stats: [numFiles=2, numRows=0, totalSize=146, rawDataSize=0]
		// OK
		// Time taken: 0.694 seconds
		// hive (db_test)> select * from employees;
		// Warning: fs.defaultFS is not set when running "chgrp" command.
		// Warning: fs.defaultFS is not set when running "chmod" command.
		// OK
		// employees.id employees.name employees.country employees.state
		// 1 'xjp1' 'china1' 'bj1'
		// 2 'xjp2' 'china2' 'bj2'
		// 3 'xjp3' 'china4' 'bj3'
		// 1 'xjp1' 'china1' 'bj1'
		// 2 'xjp2' 'china2' 'bj2'
		// 3 'xjp3' 'china4' 'bj3'
		// Time taken: 0.119 seconds, Fetched: 6 row(s)
		// hive (db_test)>

	}

	@Test
	public void test3() {

	}
}
