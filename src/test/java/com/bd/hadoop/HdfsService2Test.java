package com.bd.hadoop;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hadoop.hdfs.HdfsService;

/**
 * 测试分块
 * 
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HdfsService2Test {
	@Autowired
	private HdfsService hdfsService;

	private String path = "/hadoop/test/hadoop-hadoop-namenode-auth-sit.log";
	private String localfile = "D:/project/test/bd-demo/src/test/java/com/bd/hadoop/hadoop-hadoop-namenode-auth-sit.log";

	@Test
	public void uploadFile() {// done
		// hadoop-hadoop-namenode-auth-sit文件大小11.1 MB

		// <!-- 修改成5M,方便测试 -->
		// <property>
		// <name>dfs.block.size</name>
		// <value>5242880</value>
		// </property>
		hdfsService.uploadFile(path, localfile);

		// [hadoop@auth-sit bigdata]$ hdfs fsck /hadoop/test/hadoop-hadoop-namenode-auth-sit.log -files -blocks
		// Connecting to namenode via http://auth-sit:50070
		// FSCK started by hadoop (auth:SIMPLE) from /172.29.2.173 for path /hadoop/test/hadoop-hadoop-namenode-auth-sit.log at Tue May 29
		// 16:21:22 CST 2018
		// /hadoop/test/hadoop-hadoop-namenode-auth-sit.log 11671281 bytes, 3 block(s): OK
		// 0. BP-168566421-172.29.2.173-1527561768616:blk_1073742058_1234 len=5242880 Live_repl=2
		// 1. BP-168566421-172.29.2.173-1527561768616:blk_1073742059_1235 len=5242880 Live_repl=2
		// 2. BP-168566421-172.29.2.173-1527561768616:blk_1073742060_1236 len=1185521 Live_repl=2
		//
		// Status: HEALTHY
		// Total size: 11671281 B
		// Total dirs: 0
		// Total files: 1
		// Total symlinks: 0
		// Total blocks (validated): 3 (avg. block size 3890427 B)
		// Minimally replicated blocks: 3 (100.0 %)
		// Over-replicated blocks: 0 (0.0 %)
		// Under-replicated blocks: 0 (0.0 %)
		// Mis-replicated blocks: 0 (0.0 %)
		// Default replication factor: 3
		// Average block replication: 2.0
		// Corrupt blocks: 0
		// Missing replicas: 0 (0.0 %)
		// Number of data-nodes: 2
		// Number of racks: 1
		// FSCK ended at Tue May 29 16:21:22 CST 2018 in 1 milliseconds
		//
		//
		// The filesystem under path '/hadoop/test/hadoop-hadoop-namenode-auth-sit.log' is HEALTHY
		// [hadoop@auth-sit bigdata]$
	}

	@Test
	public void downloadFile() {// done
		hdfsService.downloadFile(path, "D:/project/test/bd-demo/src/test/java/com/bd/hadoop/hadoop-hadoop-namenode-auth-sit_down.log");
		// 结果：hadoop-hadoop-namenode-auth-sit_down.log文件大小11.1 MB
	}
}
