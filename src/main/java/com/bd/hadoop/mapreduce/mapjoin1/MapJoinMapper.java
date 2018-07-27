package com.bd.hadoop.mapreduce.mapjoin1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * join操作，map端连接。
 * 
 * @author yuan.li
 *
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Map<String, String> allCustomers = new HashMap<String, String>();

	// 启动,初始化客户信息
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fis = fs.open(new Path("/hadoop/test/customers.txt"));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis)); // 得到缓冲区阅读器
			String line = null;
			while ((line = br.readLine()) != null) {
				String cid = line.substring(0, line.indexOf(",")); // 得到cid
				allCustomers.put(cid, line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString(); // 订单信息
		String cid = line.substring(line.lastIndexOf(",") + 1);// 提取customer id
		String orderInfo = line.substring(0, line.lastIndexOf(",")); // 订单信息

		String customerInfo = allCustomers.get(cid); // 连接customer + "," + order
		context.write(new Text(customerInfo + "," + orderInfo), NullWritable.get());
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPaths = "/hadoop/test/orders.txt";
		String outputPath = "/hadoop/test/customers_out";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapJoinMapper");

		job.setJarByClass(MapJoinMapper.class); // 搜索类
		FileInputFormat.addInputPath(job, new Path(inputPaths));// 添加输入路径
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // 设置输出路径
		// 没有reduce

		job.setNumReduceTasks(0);
		job.setMapperClass(MapJoinMapper.class); // mapper类
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.waitForCompletion(true);

		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/customers_out/part-m-00000
		// 1,tom,12,1,no001,12.23
		// 1,tom,12,2,no001,12.23
		// 2,tom,13,3,no001,12.23
		// 2,tom,13,4,no001,12.23
		// 2,tom,13,5,no001,12.23
		// 3,tom,14,6,no001,12.23
		// 3,tom,14,7,no001,12.23
		// 3,tom,14,8,no001,12.23
		// 3,tom,14,9,no001,12.23
		// [hadoop@auth-sit bigdata]$
	}

}
