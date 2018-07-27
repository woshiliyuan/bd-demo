package com.bd.hive;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 通过导入一个本地文件到hive，分析hive表和hdfs关系,并分析主要Metastore结构
 * 
 * hive表：hive_test
 * 
 * 本地文件：hive_hdfs.txt
 * 
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class _1_LocalFile2HiveTest {

	@Autowired
	private HiveUtils hiveUtils;

	/**
	 * 第1步：hive->创建表hive_test
	 */
	@Test
	public void test1() {
		String sql = "create table hive_test (id int comment 'id',name string comment 'name',country string comment 'country',state string comment 'state')row format delimited fields terminated by ','";
		hiveUtils.execute(sql);

		// hive (db_test)> describe hive_test;
		// OK
		// col_name data_type comment
		// id int id
		// name string name
		// country string country
		// state string state
		// Time taken: 0.324 seconds, Fetched: 4 row(s)
		// hive (db_test)>

	}

	/**
	 * 第2步：文件->hive,导入hive_hdfs.txt文件->hive_test表
	 */
	@Test
	public void test2() {
		String sql = "load data local inpath '/apps/svr/bigdata/hive/hive_hdfs.txt' overwrite into table hive_test";
		hiveUtils.execute(sql);

		// hive (db_test)> select * from hive_test;
		// OK
		// hive_test.id hive_test.name hive_test.country hive_test.state
		// 1 'xjp1' 'china1' 'bj1'
		// 2 'xjp2' 'china2' 'bj2'
		// 3 'xjp3' 'china4' 'bj3'
		// Time taken: 0.165 seconds, Fetched: 3 row(s)
		// hive (db_test)>

	}

	/**
	 * 第3步：hive，mysql，分析主要Metastore结构
	 */
	@Test
	public void test3() {
		// SELECT * FROM cds;
		// CD_ID
		// 2
		// 3

		// SELECT * FROM dbs;
		// DB_ID DESC DB_LOCATION_URI NAME OWNER_NAME OWNER_TYPE
		// 1 Default Hive database hdfs://mycluster/hive/warehouse default public ROLE
		// 2 \N hdfs://mycluster/hive/warehouse/db_test.db db_test hadoop USER

		// SELECT * FROM tbls t WHERE t.TBL_ID=3;
		// TBL_ID CREATE_TIME DB_ID LAST_ACCESS_TIME OWNER RETENTION SD_ID TBL_NAME TBL_TYPE VIEW_EXPANDED_TEXT VIEW_ORIGINAL_TEXT
		// LINK_TARGET_ID
		// 3 1529048103 2 0 hadoop 0 3 hive_test MANAGED_TABLE \N \N \N

		// SELECT * FROM table_params t WHERE t.TBL_ID=3;
		// TBL_ID PARAM_KEY PARAM_VALUE
		// 3 COLUMN_STATS_ACCURATE true
		// 3 numFiles 1
		// 3 numRows 0
		// 3 rawDataSize 0
		// 3 totalSize 73
		// 3 transient_lastDdlTime 1529048226

		// SELECT * FROM columns_v2 t WHERE t.CD_ID=3;
		// CD_ID COMMENT COLUMN_NAME TYPE_NAME INTEGER_IDX
		// 3 country country string 2
		// 3 id id int 0
		// 3 name name string 1
		// 3 state state string 3

	}

	/**
	 * 第4步：hive->hadoop，基于第3布分析二者映射关系
	 * 
	 * 分析结果：1.本地文件上传到了hdfs，2.hive查询内容映射到了hdfs上的文件
	 */
	@Test
	public void test4() {
		// 由第3步dbs和tbls可知test_hive表文件所在文件夹路径
		// 为hdfs://mycluster/hive/warehouse/db_test.db/hive_test

		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hive/warehouse/db_test.db/hive_test
		// Found 1 items
		// -rwxr-xr-x 3 hadoop supergroup 73 2018-06-15 15:37 /hive/warehouse/db_test.db/hive_test/hive_hdfs.txt
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hive/warehouse/db_test.db/hive_test/hive_hdfs.txt
		// 1,'xjp1','china1','bj1'
		// 2,'xjp2','china2','bj2'
		// 3,'xjp3','china4','bj3'
		// [hadoop@auth-sit bigdata]$

		// 由columns_v2的INTEGER_IDX可知表字段
		// 和hdfs://mycluster/hive/warehouse/db_test.db/test_hive/hive_hdfs.txt列的映射关系
	}
}
