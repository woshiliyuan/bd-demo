package com.bd.hive;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * hdfs导入到hive，注意：发生了文件移动,无论建表时是否使用external
 * 
 * hive表：hive_test2
 * 
 * hdfs文件：/hadoop/test/hive_hdfs2.txt,内容见bd-demo/src/test/java/com/bd/hive/hive_hdfs.txt
 * 
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class _1_Hdfs2HiveTest {

	@Autowired
	private HiveUtils hiveUtils;

	/**
	 * 第1步：hive->创建表hive_test2
	 */
	@Test
	public void test1() {
		String sql = "create table hive_test2 (id int comment 'id',name string comment 'name',country string comment 'country',state string comment 'state')row format delimited fields terminated by ','";

		hiveUtils.execute(sql);
	}

	/**
	 * 第2步：hdfs->hive，hdfs导入到hive
	 */
	@Test
	public void test2() {
		// hive (db_test)> load data inpath '/hadoop/test/hive_hdfs2.txt' into table hive_test2;
		// Loading data to table db_test.hive_test2
		// Table db_test.hive_test2 stats: [numFiles=1, totalSize=73]
		// OK
		// Time taken: 1.456 seconds
		// hive (db_test)> select * from hive_test2;
		// OK
		// hive_test2.id hive_test2.name hive_test2.country hive_test2.state
		// 1 'xjp1' 'china1' 'bj1'
		// 2 'xjp2' 'china2' 'bj2'
		// 3 'xjp3' 'china4' 'bj3'
		// Time taken: 1.48 seconds, Fetched: 3 row(s)
		// hive (db_test)>
	}

	/**
	 * 第3步：hive，mysql，分析主要Metastore结构
	 */
	@Test
	public void test3() {
		// SELECT * FROM tbls t WHERE t.TBL_ID=4;
		// TBL_ID CREATE_TIME DB_ID LAST_ACCESS_TIME OWNER RETENTION SD_ID TBL_NAME TBL_TYPE VIEW_EXPANDED_TEXT VIEW_ORIGINAL_TEXT
		// LINK_TARGET_ID
		// 4 1529054344 2 0 hadoop 0 4 hive_test2 MANAGED_TABLE \N \N \N
	}

	/**
	 * 第4步：hive->hadoop
	 * 
	 * 由第3步MANAGED_TABLE和第4步可知，很坑，/hadoop/test上的hive_hdfs2.txt文件被移到/hive/warehouse/db_test.db/
	 * 
	 */
	@Test
	public void test4() {
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test
		// Found 10 items
		// -rw-r--r-- 2 hadoop supergroup 38 2018-06-08 10:48 /hadoop/test/customers.txt
		// drwxr-xr-x - hadoop supergroup 0 2018-06-08 10:56 /hadoop/test/customers_out
		// drwxr-xr-x - hadoop supergroup 0 2018-06-08 11:51 /hadoop/test/customers_out2
		// -rw-r--r-- 2 hadoop supergroup 11671281 2018-05-29 16:21 /hadoop/test/hadoop-hadoop-namenode-auth-sit.log
		// drwxr-xr-x - hadoop supergroup 0 2018-05-29 19:05 /hadoop/test/hadoop-hadoop-namenode-auth-sit_loganalysis
		// -rw-r--r-- 2 hadoop supergroup 151 2018-06-08 10:53 /hadoop/test/orders.txt
		// -rw-r--r-- 2 hadoop supergroup 533 2018-06-08 10:55 /hadoop/test/sample1.txt
		// -rw-r--r-- 2 hadoop supergroup 2372 2018-05-29 16:59 /hadoop/test/sample2.txt
		// drwxr-xr-x - hadoop supergroup 0 2018-05-29 16:59 /hadoop/test/sample2_out
		// -rw-r--r-- 2 hadoop supergroup 73 2018-06-14 16:52 /hadoop/test/test1_hdfs.txt
		// [hadoop@auth-sit bigdata]$
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hive/warehouse/db_test.db/hive_test2
		// Found 1 items
		// -rwxr-xr-x 2 hadoop supergroup 73 2018-06-15 17:23 /hive/warehouse/db_test.db/hive_test2/hive_hdfs2.txt
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hive/warehouse/db_test.db/hive_test2/hive_hdfs2.txt
		// 1,'xjp1','china1','bj1'
		// 2,'xjp2','china2','bj2'
		// 3,'xjp3','china4','bj3'
		// [hadoop@auth-sit bigdata]$
	}
}
