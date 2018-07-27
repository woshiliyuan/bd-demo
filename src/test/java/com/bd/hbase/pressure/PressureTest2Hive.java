package com.bd.hbase.pressure;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hive.HiveUtils;

/**
 * hive性能测试
 * 
 * @author yuan.li
 * 
 *         输出见main.log
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class PressureTest2Hive {
	public static Logger logger = LoggerFactory
			.getLogger(PressureTest2Hive.class);

	@Autowired
	private HiveUtils hiveUtils;

	/**
	 * 外表
	 * 
	 * hive->创建外部表hive_cust2
	 */
	@Test
	public void test1_1() {
		//@formatter:off
		/**		
		hive (db_test)> use db_test;
		OK
		Time taken: 0.189 seconds
		hive (db_test)> create external table hive_cust2(
		              > key string,
		              > name1 string comment'name1',
		              > name2 string comment 'name2',
		              > name3 string comment'name3',
		              > name4 string comment 'name4',
		              > name5 string comment'name5',
		              > addr1 string comment 'addr1',
		              > addr2 string comment'addr2',
		              > addr3 string comment 'addr3',
		              > addr40 string comment'addr40',
		              > addr50 string comment 'addr50'
		              > )stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with
		              > serdeproperties 
		              > ("hbase.columns.mapping"="info:name1,info:name2,info:name3,info:name4,info:name5,addr:addr1,addr:addr2,addr:addr3,addr:addr40,addr:addr50")
		              > tblproperties("hbase.table.name" = "cust2");
		OK
		Time taken: 3.817 seconds
		hive (db_test)> 
		*/ 
		//@formatter:on
	}

	/**
	 * 外表
	 * 
	 * 随机访问性能测试
	 * 
	 */
	@Test
	public void test1_2() {
		// query1
		// 查询条件不要没有rowkey，不然卡死你
		// 查询条件可以是rowkey和qualifier的组合
		logger.info("test1_2 query1,随机访问性能测试");
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);

			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			String sql = "select * from hive_cust2 t where t.key=" + "'"
					+ rowKey + "'";
			long st = System.currentTimeMillis();
			List<Map<String, Object>> list = hiveUtils.executeQuery(sql);
			logger.info("test1_2 query1,rowKey:{},count:{},cost time(ms):{}",
					rowKey, list == null ? 0 : list.size(),
					System.currentTimeMillis() - st);
		}
		// query2
		// 查询条件不要使用rowkey like 'ly000000001%'，不然卡死你
		// 查询条件可以是rowkey<,>,=等
		logger.info("test1_2 query2,区间访问性能测试");
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 1000);

			String rowKey1 = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 9, "0");
			String rowKey2 = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 9, "0")
					+ "z";

			String sql = "select * from hive_cust2 t where t.key>=" + "'"
					+ rowKey1 + "'" + " and t.key<=" + "'" + rowKey2 + "'";
			long st = System.currentTimeMillis();
			List<Map<String, Object>> list = hiveUtils.executeQuery(sql);
			logger.info(
					"test1_2 query2,rowKey1:{},rowKey2:{},count:{},cost time(ms):{}",
					rowKey1, rowKey2, list == null ? 0 : list.size(),
					System.currentTimeMillis() - st);
		}
	}

	/**
	 * 外表
	 * 
	 * 查看表的字段信息及元数据存储路径
	 */
	@Test
	public void test1_3() {
		//@formatter:off
  		/**	
		hive (db_test)> desc formatted hive_cust2;
		OK
		col_name        data_type       comment
		# col_name              data_type               comment             
		                 
		key                     string                  from deserializer   
		name1                   string                  from deserializer   
		name2                   string                  from deserializer   
		name3                   string                  from deserializer   
		name4                   string                  from deserializer   
		name5                   string                  from deserializer   
		addr1                   string                  from deserializer   
		addr2                   string                  from deserializer   
		addr3                   string                  from deserializer   
		addr40                  string                  from deserializer   
		addr50                  string                  from deserializer   
		                 
		# Detailed Table Information             
		Database:               db_test                  
		Owner:                  hadoop                   
		CreateTime:             Mon Jul 23 15:11:24 CST 2018     
		LastAccessTime:         UNKNOWN                  
		Protect Mode:           None                     
		Retention:              0                        
		Location:               hdfs://mycluster/hive/warehouse/db_test.db/hive_cust2    
		Table Type:             EXTERNAL_TABLE           
		Table Parameters:                
		        COLUMN_STATS_ACCURATE   false               
		        EXTERNAL                TRUE                
		        hbase.table.name        cust2               
		        numFiles                0                   
		        numRows                 -1                  
		        rawDataSize             -1                  
		        storage_handler         org.apache.hadoop.hive.hbase.HBaseStorageHandler
		        totalSize               0                   
		        transient_lastDdlTime   1532329884          
		                 
		# Storage Information            
		SerDe Library:          org.apache.hadoop.hive.hbase.HBaseSerDe  
		InputFormat:            null                     
		OutputFormat:           null                     
		Compressed:             No                       
		Num Buckets:            -1                       
		Bucket Columns:         []                       
		Sort Columns:           []                       
		Storage Desc Params:             
		        hbase.columns.mapping   info:name1,info:name2,info:name3,info:name4,info:name5,addr:addr1,addr:addr2,addr:addr3,addr:addr40,addr:addr50
		        serialization.format    1                   
		Time taken: 0.42 seconds, Fetched: 45 row(s)
		hive (db_test)> 
		*/
	}

	/**
	 * 内表
	 * 
	 * 创建hive表hive_cust22
	 */
	@Test
	public void test2_1() {
		//@formatter:off
  		/**	
		hive (db_test)> create table hive_cust22 (
	              > key string,
	              > name1 string comment'name1',
	              > name2 string comment 'name2',
	              > name3 string comment'name3',
	              > name4 string comment 'name4',
	              > name5 string comment'name5',
	              > addr1 string comment 'addr1',
	              > addr2 string comment'addr2',
	              > addr3 string comment 'addr3',
	              > addr40 string comment'addr40',
	              > addr50 string comment 'addr50'
	              > )row format delimited
	              > fields terminated by '\t'
	              > stored as textfile;
		OK
		Time taken: 1.152 seconds
		hive (db_test)> 
	    */ 
		//@formatter:on      
	}

	/**
	 * 内表
	 * 
	 * 抽数性能分析
	 * 
	 * 大约12分钟/1千万条
	 */
	@Test
	public void test2_2() {
		/********************************************************************/
		// execute1
		// [ly0000000000,ly0010000000)
		logger.info("test2_2,抽数性能分析");
		long st1 = System.currentTimeMillis();
		String sql1 = "insert overwrite table hive_cust22 select t.* from hive_cust2 t "
				+ "where t.key>='ly0000000000' and t.key<'ly0010000000'";
		hiveUtils.execute(sql1);
		logger.info("test2_2 execute1,sql:{},cost time(ms):{}", sql1,
				System.currentTimeMillis() - st1);
		/********************************************************************/
		// execute2
		// [ly0010000000,ly0020000000)
		logger.info("test2_2,抽数性能分析");
		long st2 = System.currentTimeMillis();
		String sql2 = "insert into table hive_cust22 select t.* from hive_cust2 t "
				+ "where t.key>='ly0010000000' and t.key<'ly0020000000'";
		hiveUtils.execute(sql2);
		logger.info("test2_2 execute2,sql:{},cost time(ms):{}", sql2,
				System.currentTimeMillis() - st2);
		/********************************************************************/
		// execute3
		logger.info("test2_2,创建索引");
		long st3 = System.currentTimeMillis();
		String sql3 = "create index hive_cust22_index on table hive_cust22(key) as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' with deferred rebuild in table user_index_table";
		hiveUtils.execute(sql3);
		logger.info("test2_2 execute3,sql:{},cost time(ms):{}", sql3,
				System.currentTimeMillis() - st3);

	}

	/**
	 * 内表,随机访问性能测试
	 * 
	 * 建了索引，还是全表扫描???
	 */
	@Test
	// TODO
	public void test2_3() {
		// query1
		logger.info("test2_3 query1,随机访问性能测试");
		for (int i = 0; i < 4; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);

			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			String sql = "select * from hive_cust22 t where t.key=" + "'"
					+ rowKey + "'";
			long st = System.currentTimeMillis();
			List<Map<String, Object>> list = hiveUtils.executeQuery(sql);
			logger.info("test2_3 query1,rowKey:{},count:{},cost time(ms):{}",
					rowKey, list == null ? 0 : list.size(),
					System.currentTimeMillis() - st);
		}
	}

	/**
	 * 内表
	 * 
	 * 查看表的字段信息及元数据存储路径
	 */
	@Test
	public void test2_4() {
		//@formatter:off
		/**
		hive (db_test)> desc formatted hive_cust22;
		OK
		col_name        data_type       comment
		# col_name              data_type               comment             
		                 
		key                     string                                      
		name1                   string                  name1               
		name2                   string                  name2               
		name3                   string                  name3               
		name4                   string                  name4               
		name5                   string                  name5               
		addr1                   string                  addr1               
		addr2                   string                  addr2               
		addr3                   string                  addr3               
		addr40                  string                  addr40              
		addr50                  string                  addr50              
		                 
		# Detailed Table Information             
		Database:               db_test                  
		Owner:                  hadoop                   
		CreateTime:             Mon Jul 23 18:05:13 CST 2018     
		LastAccessTime:         UNKNOWN                  
		Protect Mode:           None                     
		Retention:              0                        
		Location:               hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22   
		Table Type:             MANAGED_TABLE            
		Table Parameters:                
		        COLUMN_STATS_ACCURATE   true                
		        numFiles                30                  
		        numRows                 20000000            
		        rawDataSize             1346873936          
		        totalSize               1366873936          
		        transient_lastDdlTime   1532342064          
		                 
		# Storage Information            
		SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
		InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
		OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
		Compressed:             No                       
		Num Buckets:            -1                       
		Bucket Columns:         []                       
		Sort Columns:           []                       
		Storage Desc Params:             
		        field.delim             \t                  
		        serialization.format    \t                  
		Time taken: 1.438 seconds, Fetched: 42 row(s)
		hive (db_test)> 
		*/
		/**
		[hadoop@auth-sit bigdata]$ hadoop fs -ls hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22
			Found 30 items
			-rwxr-xr-x   3 hadoop supergroup   53910116 2018-07-23 18:16 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000000_0
			-rwxr-xr-x   3 hadoop supergroup   84696936 2018-07-23 18:34 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000000_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   83916544 2018-07-23 18:16 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000001_0
			-rwxr-xr-x   3 hadoop supergroup   63606222 2018-07-23 18:32 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000001_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup  171881217 2018-07-23 18:14 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000002_0
			-rwxr-xr-x   3 hadoop supergroup   60086840 2018-07-23 18:32 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000002_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   44152964 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000003_0
			-rwxr-xr-x   3 hadoop supergroup   46695046 2018-07-23 18:32 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000003_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   43313704 2018-07-23 18:14 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000004_0
			-rwxr-xr-x   3 hadoop supergroup   46656882 2018-07-23 18:30 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000004_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   44777507 2018-07-23 18:14 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000005_0
			-rwxr-xr-x   3 hadoop supergroup   46057510 2018-07-23 18:30 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000005_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   39586044 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000006_0
			-rwxr-xr-x   3 hadoop supergroup   43354826 2018-07-23 18:29 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000006_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   39863284 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000007_0
			-rwxr-xr-x   3 hadoop supergroup    2393307 2018-07-23 18:22 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000007_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   45767681 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000008_0
			-rwxr-xr-x   3 hadoop supergroup   41291763 2018-07-23 18:29 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000008_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   38294152 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000009_0
			-rwxr-xr-x   3 hadoop supergroup   38924612 2018-07-23 18:29 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000009_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   36258004 2018-07-23 18:12 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000010_0
			-rwxr-xr-x   3 hadoop supergroup   36257134 2018-07-23 18:29 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000010_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   35826948 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000011_0
			-rwxr-xr-x   3 hadoop supergroup   36240372 2018-07-23 18:29 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000011_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   33969208 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000012_0
			-rwxr-xr-x   3 hadoop supergroup   34612486 2018-07-23 18:28 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000012_0_copy_1
			-rwxr-xr-x   3 hadoop supergroup   34343530 2018-07-23 18:12 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000013_0
			-rwxr-xr-x   3 hadoop supergroup   25287944 2018-07-23 18:16 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000014_0
			-rwxr-xr-x   3 hadoop supergroup   13317992 2018-07-23 18:14 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000015_0
			-rwxr-xr-x   3 hadoop supergroup    1533161 2018-07-23 18:13 hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000016_0
			[hadoop@auth-sit bigdata]$ 
		*/
		/**
		[hadoop@auth-sit bigdata]$ hadoop fs -cat hdfs://mycluster/hive/warehouse/db_test.db/hive_cust22/000000_0|more -10
			ly0001780002    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780003    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780004    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780005    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780006    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780007    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780008    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780009    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780010    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			ly0001780011    yuan1   yuan2   yuan3   yuan4   yuan5   addr1   addr2   addr3   \N      \N
			--More-- 
		 */
		//@formatter:on
	}
}
