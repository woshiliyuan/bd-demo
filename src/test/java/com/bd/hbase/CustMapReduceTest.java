package com.bd.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hbase.mapreduce.CustMapReduce;
import com.bd.hbase.mapreduce.CustMapReduce.MyTableMapper;
import com.bd.hbase.mapreduce.CustMapReduce.MyTableReducer;

/**
 * 
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class CustMapReduceTest {
	/**
	 * 准备数据见：data_cust.txt:scan 'cust'
	 * 
	 * 执行结果见：data_cust.txt:scan 'cust1'
	 * 
	 */
	@Test
	public void test() {// TODO not done
		String sourceTable = "cust";
		String targetTable = "cust1";
		try {
			Configuration configuration = HBaseConfiguration.create();

			Job job = Job.getInstance(configuration);
			job.setJarByClass(CustMapReduce.class); // class that contains mapper and reducer

			Scan scan = new Scan();
			scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			// set other scan attrs

			TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
					scan, // Scan instance to control CF and attribute selection
					MyTableMapper.class, // mapper class
					Text.class, // mapper output key
					IntWritable.class, // mapper output value
					job);

			TableMapReduceUtil.initTableReducerJob(targetTable, // output table
					MyTableReducer.class, // reducer class
					job);

			job.setNumReduceTasks(1); // at least one, adjust as required

			boolean b = job.waitForCompletion(true);

			if (!b) {
				throw new IOException("error with job!");
			}
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
