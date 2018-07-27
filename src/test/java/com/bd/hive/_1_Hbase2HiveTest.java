package com.bd.hive;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * hbase导入到hive，从hive建立可以访问hbase的外部表hive_cust1
 * 
 * 1.hbase表：cust，见bd-demo\src\test\java\com\bd\hbase\data_cust.txt
 * 
 * 2.hive表：hive_cust1
 * 
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class _1_Hbase2HiveTest {

	/**
	 * 第1步：hive->创建外部表hive_cust1
	 * 
	 * 1.创建hive表，建立hive和hbase的映射关系->hive_cust1，cust
	 * 
	 * 2.查询hive表：hive_cust1，由查询结果可知，hive表的内容指向hbase表的内容
	 */
	@Test
	public void test1() {

		// hive (db_test)> create external table hive_cust1(
		// > key string,
		// > firstname string comment 'firstname',
		// > secondname string comment'secondname',
		// > age int comment 'age',
		// > area string comment 'area',
		// > city string comment 'city'
		// > ) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
		// > with serdeproperties ("hbase.columns.mapping" = "info:firstname,info:secondname,info:age,addr:area,addr:city")
		// > tblproperties("hbase.table.name" = "cust");
		// OK
		// Time taken: 0.505 seconds
		// hive (db_test)> select * from hive_cust1;
		// OK
		// hive_cust1.key hive_cust1.firstname hive_cust1.secondname hive_cust1.age hive_cust1.area hive_cust1.city
		// liyuan li yuan NULL pudong shanghai
		// liyuan1 li yuan NULL pudong shanghai
		// liyuan2 li yuan NULL pudong shanghai
		// lkq l NULL 18 tiananmen beijing
		// lkq1 l NULL 18 tiananmen beijing
		// lkq2 l NULL 18 tiananmen beijing
		// xjp x jp NULL tiananmen beijing
		// xjp1 x jp NULL tiananmen beijing
		// xjp2 x jp NULL tiananmen beijing
		// Time taken: 1.056 seconds, Fetched: 9 row(s)
		// hive (db_test)>

	}

	/**
	 * 第2步：分析hive对hdfs的影响
	 * 
	 * 分析可知，1.只产生了声明作用的空的数据库和表名文件文件夹，2.数据源本身还是hbase，3.hive本身业务未消耗存储
	 * 
	 */
	@Test
	public void test2() {
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hive/warehouse/
		// Found 1 items
		// drwxr-xr-x - hadoop supergroup 0 2018-06-15 15:35 /hive/warehouse/db_test.db
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hive/warehouse/db_test.db/
		// Found 2 items
		// drwxr-xr-x - hadoop supergroup 0 2018-06-15 15:19 /hive/warehouse/db_test.db/hive_cust1
		// drwxr-xr-x - hadoop supergroup 0 2018-06-15 15:37 /hive/warehouse/db_test.db/hive_test
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hive/warehouse/db_test.db/hive_cust1/
		// [hadoop@auth-sit bigdata]$
	}

	/**
	 * 第3步：hive，mysql，分析主要Metastore结构
	 * 
	 */
	@Test
	public void test3() {
		// SELECT * FROM tbls t WHERE t.TBL_ID=2;
		// TBL_ID CREATE_TIME DB_ID LAST_ACCESS_TIME OWNER RETENTION SD_ID TBL_NAME TBL_TYPE VIEW_EXPANDED_TEXT VIEW_ORIGINAL_TEXT
		// LINK_TARGET_ID
		// 2 1529047199 2 0 hadoop 0 2 hive_cust1 EXTERNAL_TABLE \N \N \N

		// SELECT * FROM table_params t WHERE t.TBL_ID=2;
		// TBL_ID PARAM_KEY PARAM_VALUE
		// 2 EXTERNAL TRUE
		// 2 hbase.table.name cust
		// 2 storage_handler org.apache.hadoop.hive.hbase.HBaseStorageHandler
		// 2 transient_lastDdlTime 1529047199

		// SELECT * FROM columns_v2 t WHERE t.CD_ID=2;
		// CD_ID COMMENT COLUMN_NAME TYPE_NAME INTEGER_IDX
		// 2 age age int 3
		// 2 area area string 4
		// 2 city city string 5
		// 2 firstname firstname string 1
		// 2 \N key string 0
		// 2 secondname secondname string 2
	}

	/**
	 * 第4步：分析hbase对hive的数据的影响
	 * 
	 * 分析可知：当hbase中写入数据后，hive查询中也会同时更新
	 * 
	 */
	@Test
	public void test4() {
		// 1.hbase插入前
		// 1.1
		// hbase(main):002:0> scan 'cust'
		// ROW COLUMN+CELL
		// 。。。。。。。。省略
		// 9 row(s) in 0.7320 seconds
		//
		// hbase(main):003:0>
		// 1.2
		// hive (db_test)> select * from hive_cust1;
		// OK
		// hive_cust1.key hive_cust1.firstname hive_cust1.secondname hive_cust1.age hive_cust1.area hive_cust1.city
		// 。。。。。。。。省略
		// Time taken: 1.879 seconds, Fetched: 9 row(s)
		// hive (db_test)>

		// 2.向hbase插入1条数据
		// hbase(main):001:0> put 'cust', 'liyuan3', 'info:firstname', 'li3'
		// 0 row(s) in 0.8320 seconds
		//
		// hbase(main):002:0>

		// 3.查看
		// 1.1
		// hbase(main):004:0> scan 'cust'
		// ROW COLUMN+CELL
		// 。。。。。。。。省略
		// liyuan3 column=info:firstname, timestamp=1529051754552, value=li3
		// 。。。。。。。。省略
		// 10 row(s) in 0.6910 seconds
		//
		// hbase(main):005:0>
		// 1.2
		// hive (db_test)> select * from hive_cust1;
		// OK
		// hive_cust1.key hive_cust1.firstname hive_cust1.secondname hive_cust1.age hive_cust1.area hive_cust1.city
		// 。。。。。。。。省略
		// liyuan3 li3 NULL NULL NULL NULL
		// 。。。。。。。。省略
		// Time taken: 1.654 seconds, Fetched: 10 row(s)
		// hive (db_test)> ;

	}

	/**
	 * 第5步：分析hive对hbase的数据的影响
	 * 
	 * 省略。Hive是不支持更新操作的
	 */
	@Test
	public void test5() {

	}
}
