package com.bd.hbase.pressure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hbase.HBaseUtils;
import com.bd.hbase.common.Constants;
import com.bd.hbase.common.QualifierModel;

/**
 * hbase性能测试
 * 
 * @author yuan.li
 * 
 *         df.replication=3
 * 
 *         输出见main.log
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class PressureTest {
	public static Logger logger = LoggerFactory.getLogger(PressureTest.class);
	@Autowired
	private HBaseUtils hBaseUtils;
	@Autowired
	private ThreadPoolTaskExecutor taskExecutor;

	/**
	 * 查看服务器信息
	 */
	@Test
	public void test1_1() {
		//@formatter:off
		/**		
		[hadoop@auth-sit bigdata]$ df -m
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143      5688    252634   3% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4051     15085  22% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159       483     18653   3% /var
		[hadoop@auth-sit bigdata]$ free -m
		             total       used       free     shared    buffers     cached
		Mem:          3833       3099        734          0         82        126
		-/+ buffers/cache:       2890        943
		Swap:        16095       1007      15088
		[hadoop@auth-sit bigdata]$ 
		[hadoop@mpi-sit bigdata]$ df -m
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143      6321    252001   3% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4310     14825  23% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159      1006     18130   6% /var
		[hadoop@mpi-sit bigdata]$ free -m
		             total       used       free     shared    buffers     cached
		Mem:          3833       3563        270          0        237       1079
		-/+ buffers/cache:       2246       1586
		Swap:        16095         78      16017
		[hadoop@mpi-sit bigdata]$
		[hadoop@mpc-sit bigdata]$ df -m
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143      6204    252118   3% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4283     14852  23% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159      1016     18119   6% /var
		[hadoop@mpc-sit bigdata]$ free -m
		             total       used       free     shared    buffers     cached
		Mem:          3833       3637        196          0        311        973
		-/+ buffers/cache:       2352       1481
		Swap:        16095         34      16061
		[hadoop@mpc-sit bigdata]$
		*/ 
		//@formatter:on
	}

	/**
	 * 存储消耗分析
	 * 
	 * cust2，列簇info插入2千万row key
	 * 
	 * [0, 2000 * 10000)每个列簇包含100列
	 * 
	 * 耗时：10739s/1百万条
	 * 
	 * 存储：(mpi(105589-6321)m+mpc(105449-6204)m)/3个副本=66171m=64.6g
	 * 
	 */
	@Test
	public void test1_2() {
		//@formatter:off
		/**	
		hbase(main):004:0> create 'cust2', {NAME => 'info', VERSIONS => '1'}, {NAME => 'addr', VERSIONS => '1'}
		0 row(s) in 1.2660 seconds

		=> Hbase::Table - cust2
		hbase(main):005:0> describe 'cust2'
		Table cust2 is ENABLED                                                                                                                                                                              
		cust2                                                                                                                                                                                               
		COLUMN FAMILIES DESCRIPTION                                                                                                                                                                         
		{NAME => 'addr', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS =>
		 '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}                                                                                                                         
		{NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS =>
		 '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}                                                                                                                         
		2 row(s) in 0.0720 seconds

		hbase(main):006:0> 
		*/
		//@formatter:on

		logger.info("test1_2 insert start");
		long st = System.currentTimeMillis();

		long st_sub = st;

		// int start = 0;int end = 100 * 10000;[0, 100 * 10000)
		// int start = 100 * 10000;int end = 200 * 10000;
		// int start = 200 * 10000;int end = 2000 * 10000;
		int start = 1430 * 10000;
		int end = 2000 * 10000;
		for (int i = start; i < end; i++) {
			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(i), 10, "0");// 位数补齐，用于rowkey排序
			// maxPoolSize调为20
			// 每个列簇 100个qualifier
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					Map<String, String> map = new HashMap<String, String>();
					for (int j = 0; j < 100; j++) {
						String key = "name" + j;
						String value = "yuan" + j;
						map.put(key, value);
					}
					hBaseUtils.insert.putColumn("cust2", rowKey, "info", map);
				}
			});
			if (i % 1000000 == 0) {
				logger.info(
						"test1_2 insert start sub,i:[{},{}),cost time(ms):{}",
						i, i + 1000000, System.currentTimeMillis() - st_sub);
				st_sub = System.currentTimeMillis();
			}
		}
		while (taskExecutor.getActiveCount() != 0) {
		}
		logger.info("test1_2 insert end,start:{},end:{},cost time(ms):{}",
				start, end, System.currentTimeMillis() - st);
	}

	/**
	 * 列簇大小对插入性能的影响
	 * 
	 * cust2，列簇addr插入4百万row key,
	 * 
	 * [0 * 10000, 200 * 10000)每个列簇包含10列：1341s/1百万条
	 * 
	 * [200 * 10000, 300 * 10000)每个列簇包含100列：10615s/1百万条
	 * 
	 * [300 * 10000, 400 * 10000)每个列簇包含10列（value长度增加10倍）：2375s/1百万条
	 * 
	 */
	@Test
	public void test1_3() {
		/********************************************************************/
		logger.info("test1_3 sub1 insert start");
		long st = System.currentTimeMillis();

		long st_sub = st;

		int start = 0 * 10000;
		int end = 200 * 10000;// [ 0,200 * 10000)
		for (int i = start; i < end; i++) {
			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(i), 10, "0");
			// maxPoolSize调为20
			// 每个列簇 10个qualifier
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					Map<String, String> map = new HashMap<String, String>();
					for (int j = 0; j < 10; j++) {
						String key = "addr" + j;
						String value = "addr" + j;
						map.put(key, value);
					}
					hBaseUtils.insert.putColumn("cust2", rowKey, "addr", map);
				}
			});
			if (i % 1000000 == 0) {
				logger.info(
						"test1_3 sub1 insert start,i:[{},{}),cost time(ms):{}",
						i, i + 1000000, System.currentTimeMillis() - st_sub);
				st_sub = System.currentTimeMillis();
			}
		}
		while (taskExecutor.getActiveCount() != 0) {
		}
		logger.info("test1_3 sub1 insert end,start:{},end:{},cost time(ms):{}",
				start, end, System.currentTimeMillis() - st);

		/********************************************************************/
		logger.info("test1_3 sub2 insert start");
		long st2 = System.currentTimeMillis();

		int start2 = 200 * 10000;
		int end2 = 300 * 10000;// [200 * 10000, 300 * 10000)
		for (int i = start2; i < end2; i++) {
			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(i), 10, "0");
			// maxPoolSize调为20
			// 每个列簇 100个qualifier
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					Map<String, String> map = new HashMap<String, String>();
					for (int j = 0; j < 100; j++) {
						String key = "addr" + j;
						String value = "addr" + j;
						map.put(key, value);
					}
					hBaseUtils.insert.putColumn("cust2", rowKey, "addr", map);
				}
			});
		}
		while (taskExecutor.getActiveCount() != 0) {
		}
		logger.info("test1_3 sub2 insert end,start:{},end:{},cost time(ms):{}",
				start2, end2, System.currentTimeMillis() - st2);

		/********************************************************************/
		logger.info("test1_3 sub3 insert start");
		long st3 = System.currentTimeMillis();

		int start3 = 300 * 10000;
		int end3 = 400 * 10000;// [300 * 10000, 400 * 10000)

		for (int i = start3; i < end3; i++) {
			String rowKey = "ly"
					+ StringUtils.leftPad(String.valueOf(i), 10, "0");
			// maxPoolSize调为20
			// 每个列簇 10个qualifier
			// value增加约10倍
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					Map<String, String> map = new HashMap<String, String>();
					for (int j = 0; j < 10; j++) {
						String key = "addr" + j;
						String value = "addr00addr01addr02addr03addr04addr05addr06addr07addr08addr"
								+ j;
						map.put(key, value);
					}
					hBaseUtils.insert.putColumn("cust2", rowKey, "addr", map);
				}
			});
		}
		while (taskExecutor.getActiveCount() != 0) {
		}
		logger.info("test1_3 sub3 insert end,start:{},end:{},cost time(ms):{}",
				start3, end3, System.currentTimeMillis() - st3);
	}

	/**
	 * 查看服务器信息
	 */
	@Test
	public void test1_4() {
		//@formatter:off
		/**	
		[hadoop@auth-sit bigdata]$ df -m
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143      5743    252579   3% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4051     15085  22% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159       492     18643   3% /var
		[hadoop@auth-sit bigdata]$ 
		[hadoop@mpi-sit bigdata]$ df -m
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143    113074    145248  44% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4310     14825  23% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159      1035     18101   6% /var
		[hadoop@mpi-sit bigdata]$ 
		[hadoop@mpc-sit bigdata]$ df -m                
		文件系统                 1M-块      已用      可用 已用% 挂载点
		/dev/mapper/vg_app11prd-lv_root
		                        272143    112931    145391  44% /
		tmpfs                     1917         1      1917   1% /dev/shm
		/dev/sda1                  485        38       423   9% /boot
		/dev/mapper/vg_app11prd-lv_home
		                         20159      4283     14852  23% /home
		/dev/mapper/vg_app11prd-lv_var
		                         20159      1022     18113   6% /var
		[hadoop@mpc-sit bigdata]$ 	
		*/ 
		//@formatter:on
	}

	/**
	 * cust2表get随机访问性能测试
	 * 
	 * 响应时间约20ms
	 */
	@Test
	public void test2_1() {
		/********************************************************************/
		logger.info("test2_1 query1/query2/query3/query4,get随机访问性能");
		// query1
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String rowkey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			long st = System.currentTimeMillis();
			Map<String, List<QualifierModel>> result1 = hBaseUtils.query1
					.query("cust2", rowkey, null, null);
			logger.info("test2_1 query1,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result1 == null ? 0 : 1, System.currentTimeMillis()
							- st);
		}
		// query2
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String rowkey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			long st = System.currentTimeMillis();
			Map<String, List<QualifierModel>> result2 = hBaseUtils.query1
					.query("cust2", rowkey, "info", null);
			logger.info("test2_1 query2,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result2 == null ? 0 : 1, System.currentTimeMillis()
							- st);
		}
		// query3
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String rowkey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			long st = System.currentTimeMillis();
			Map<String, String> params = new HashMap<String, String>();
			params.put(Constants.params.startColumn, "name90");
			params.put(Constants.params.endColumn, "name99");
			Map<String, List<QualifierModel>> result3 = hBaseUtils.query1
					.query("cust2", rowkey, "info", params);
			logger.info("test2_1 query3,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result3 == null ? 0 : 1, System.currentTimeMillis()
							- st);
		}
		// query4
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String rowkey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			long st = System.currentTimeMillis();

			Map<String, List<QualifierModel>> result4 = hBaseUtils.query1
					.query("cust2", rowkey, "info", null, "name99");
			logger.info("test2_1 query4,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result4 == null ? 0 : 1, System.currentTimeMillis()
							- st);
		}
		/********************************************************************/
		logger.info("test2_1 query5,get随机访问性能统计");
		// query5
		// 范围[0,400*10000)
		// 分析结果：cost1>cost2>cost3>cost4
		for (int i = 0; i < 10; i++) {
			long cost1 = 0l;// 多列簇查找
			long cost2 = 0l;// 单列簇查找
			long cost3 = 0l;// 单列簇，指定列查范围查找
			long cost4 = 0l;// 单列簇，指定列查找
			// cost1
			long st = System.currentTimeMillis();
			for (int j = 0; j < 1000; j++) {
				Random rand = new Random();
				int nextInt = rand.nextInt(400 * 10000);
				String rowkey = "ly"
						+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

				hBaseUtils.query1.query("cust2", rowkey, null, null);

			}
			cost1 = System.currentTimeMillis() - st;
			// cost2
			st = System.currentTimeMillis();
			for (int j = 0; j < 1000; j++) {
				Random rand = new Random();
				int nextInt = rand.nextInt(400 * 10000);
				String rowkey = "ly"
						+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
				hBaseUtils.query1.query("cust2", rowkey, "info", null);
			}
			cost2 = System.currentTimeMillis() - st;
			// cost3
			st = System.currentTimeMillis();
			for (int j = 0; j < 1000; j++) {
				Random rand = new Random();
				int nextInt = rand.nextInt(400 * 10000);
				String rowkey = "ly"
						+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
				Map<String, String> params = new HashMap<String, String>();
				params.put(Constants.params.startColumn, "name90");
				params.put(Constants.params.endColumn, "name99");
				hBaseUtils.query1.query("cust2", rowkey, "info", params);

			}
			cost3 = System.currentTimeMillis() - st;
			// cost4
			st = System.currentTimeMillis();
			for (int j = 0; j < 1000; j++) {
				Random rand = new Random();
				int nextInt = rand.nextInt(400 * 10000);
				String rowkey = "ly"
						+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
				hBaseUtils.query1
						.query("cust2", rowkey, "info", null, "name99");

			}
			cost4 = System.currentTimeMillis() - st;
			logger.info(
					"test2_1 query5,cost1 time(ms):{},cost2 time(ms):{},cost3 time(ms):{},cost4 time(ms):{}",
					cost1, cost2, cost3, cost4);
		}
		/********************************************************************/
		logger.info("test2_1 query6,get查询热点数据对性能的影响");
		// query5
		// 分析结果，好像没有做热点数据查询优化
		for (int i = 0; i < 4; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(2000 * 10000);
			String rowkey = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
			// sub1
			for (int j = 0; j < 4; j++) {
				long st = System.currentTimeMillis();
				Map<String, List<QualifierModel>> result4 = hBaseUtils.query1
						.query("cust2", rowkey, "info", null);
				logger.info(
						"test2_1 query6 sub1,rowkey:{},count:{},cost time(ms):{}",
						rowkey, result4 == null ? 0 : 1,
						System.currentTimeMillis() - st);
			}
			// sub2
			for (int j = 0; j < 4; j++) {
				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				long st = System.currentTimeMillis();
				Map<String, List<QualifierModel>> result4 = hBaseUtils.query1
						.query("cust2", rowkey, "info", null);
				logger.info(
						"test2_1 query6 sub2,rowkey:{},count:{},cost time(ms):{}",
						rowkey, result4 == null ? 0 : 1,
						System.currentTimeMillis() - st);
			}
			// sub3
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long st = System.currentTimeMillis();
			Map<String, List<QualifierModel>> result4 = hBaseUtils.query1
					.query("cust2", rowkey, "info", null);
			logger.info(
					"test2_1 query6 sub3,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result4 == null ? 0 : 1, System.currentTimeMillis()
							- st);
			// sub4
			try {
				Thread.sleep(60 * 1000 * 12);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			st = System.currentTimeMillis();
			Map<String, List<QualifierModel>> result5 = hBaseUtils.query1
					.query("cust2", rowkey, "info", null);
			logger.info(
					"test2_1 query6 sub4,rowkey:{},count:{},cost time(ms):{}",
					rowkey, result5 == null ? 0 : 1, System.currentTimeMillis()
							- st);
		}
	}

	/**
	 * cust2表scan随机访问性能测试
	 * 
	 * 响应时间约10ms/条
	 */
	@Test
	public void test2_2() {
		/********************************************************************/
		logger.info("test2_2 query1/query2/query3/query4,scan随机访问性能");
		// query1
		// 1条
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String startRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
			String stopRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");

			long st = System.currentTimeMillis();

			Map<String, String> params = new HashMap<String, String>();
			params.put(Constants.params.startRow, startRow);
			params.put(Constants.params.stopRow, stopRow);
			Map<String, Map<String, String>> result1 = hBaseUtils.query2.query(
					"cust2", params);
			logger.info("test2_2 query1,count:{},cost time(ms):{}",
					result1 == null ? 0 : result1.size(),
					System.currentTimeMillis() - st);
		}
		// query2
		// 10条
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String startRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
			String stopRow = "ly"
					+ StringUtils
							.leftPad(String.valueOf(nextInt + 10), 10, "0");

			long st = System.currentTimeMillis();

			Map<String, String> params = new HashMap<String, String>();
			params.put(Constants.params.startRow, startRow);
			params.put(Constants.params.stopRow, stopRow);
			Map<String, Map<String, String>> result1 = hBaseUtils.query2.query(
					"cust2", params);
			logger.info("test2_2 query2,count:{},cost time(ms):{}",
					result1 == null ? 0 : result1.size(),
					System.currentTimeMillis() - st);
		}
		// query3
		// 100条
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String startRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
			String stopRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt + 100), 10,
							"0");

			long st = System.currentTimeMillis();

			Map<String, String> params = new HashMap<String, String>();
			params.put(Constants.params.startRow, startRow);
			params.put(Constants.params.stopRow, stopRow);
			Map<String, Map<String, String>> result1 = hBaseUtils.query2.query(
					"cust2", params);
			logger.info("test2_2 query3,count:{},cost time(ms):{}",
					result1 == null ? 0 : result1.size(),
					System.currentTimeMillis() - st);
		}
		// query4
		// 1000条
		for (int i = 0; i < 10; i++) {
			Random rand = new Random();
			int nextInt = rand.nextInt(3000 * 10000);
			String startRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt), 10, "0");
			String stopRow = "ly"
					+ StringUtils.leftPad(String.valueOf(nextInt + 1000), 10,
							"0");

			long st = System.currentTimeMillis();

			Map<String, String> params = new HashMap<String, String>();
			params.put(Constants.params.startRow, startRow);
			params.put(Constants.params.stopRow, stopRow);
			Map<String, Map<String, String>> result1 = hBaseUtils.query2.query(
					"cust2", params);
			logger.info("test2_2 query4,count:{},cost time(ms):{}",
					result1 == null ? 0 : result1.size(),
					System.currentTimeMillis() - st);
		}

	}

	/**
	 * cust2表数据结构信息
	 */
	@Test
	public void test3_1() {
		//@formatter:off
		/**
		hadoop fs -cat /hbase/data/default/cust2/1598a847aa4450c208bf247037df7e6e/info/0b552756fbbe4d71b095aba97fa7b293
		ly0017121133infoname47dyuan47n"
		ly0017121133infoname48dyuan48n"
		ly0017121133infoname49dyuan49n!
		ly0017121133infoname5dyuan5n"
		ly0017121133infoname50dyuan50n"
		ly0017121133infoname51dyuan51n"
		ly0017121133infoname52dyuan52n"
		ly0017121133infoname53dyuan53n"
		ly0017121133infoname54dyuan54n"
		ly0017121133infoname55dyuan55n"
		 */
		/**
		[hadoop@auth-sit bigdata]$ hadoop fs -du -s /hbase
		111581308361  335549230953  /hbase
		[hadoop@auth-sit bigdata]$ 
		 */
		//@formatter:on
	}
}
