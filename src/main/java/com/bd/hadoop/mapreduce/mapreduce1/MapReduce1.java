package com.bd.hadoop.mapreduce.mapreduce1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author yuan.li
 *
 */
public class MapReduce1 {

	/**
	 * key：map方法把文件的行号当成key,所以要用LongWritable。
	 * 
	 * value：该行的内容
	 * 
	 * context：
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("mytestmap:" + "==========");
			// System.out.println("mytestmap:" + key + "-" + value);
			String line = value.toString(); // 订单信息
			String[] arr = line.split(",");
			context.write(new Text(arr[1]), new Text(arr[2]));// 姓名，金额
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("mytestreduce:" + "==========");
			Iterator<Text> it = values.iterator();
			Integer sum = 0;
			while (it.hasNext()) {
				String v = it.next().toString();
				sum += Integer.valueOf(v);
				System.out.println("mytestreduce:" + key + "-" + v);
			}
			context.write(key, new Text(sum.toString()));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {// done
		String inputPaths = "/hadoop/test/sample3.txt";
		String outputPath = "/hadoop/test/sample3.out";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduce1");
		job.setJarByClass(MapReduce1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPaths));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// 以下通过setPartitionerClass，setGroupingComparatorClass，setNumReduceTasks三者不同组合对结果输出的影响做出分析
		/**
		 * 1
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 2 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:08 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 54 2018-06-26 16:08 /hadoop/test/sample3.out/part-r-00000
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom1 211
		// tom2 209
		// tom3 108
		// tom4 103
		// tom5 109
		// tom6 205
		// [hadoop@auth-sit bigdata]$
		//
		// mytestmap:0-1,tom6,101
		// mytestmap:==========
		// mytestmap:12-2,tom2,102
		// mytestmap:==========
		// mytestmap:24-3,tom4,103
		// mytestmap:==========
		// mytestmap:36-4,tom6,104
		// mytestmap:==========
		// mytestmap:48-5,tom1,105
		// mytestmap:==========
		// mytestmap:60-6,tom1,106
		// mytestmap:==========
		// mytestmap:72-7,tom2,107
		// mytestmap:==========
		// mytestmap:84-8,tom3,108
		// mytestmap:==========
		// mytestmap:96-9,tom5,109
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom4-103
		// mytestreduce:==========
		// mytestreduce:tom5-109
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		/**
		 * 2
		 * 
		 * job.setSortComparatorClass(MySortComparator.class);
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 2 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:10 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 54 2018-06-26 16:10 /hadoop/test/sample3.out/part-r-00000
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom6 205
		// tom5 109
		// tom4 103
		// tom3 108
		// tom2 209
		// tom1 211
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		// mytestreduce:==========
		// mytestreduce:tom5-109
		// mytestreduce:==========
		// mytestreduce:tom4-103
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		/**
		 * 3.1
		 * 
		 * job.setPartitionerClass(MyPartitioner.class);
		 * 
		 * job.setNumReduceTasks(2);
		 */
		//
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 3 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:13 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 27 2018-06-26 16:13 /hadoop/test/sample3.out/part-r-00000
		// -rw-r--r-- 2 hadoop supergroup 27 2018-06-26 16:13 /hadoop/test/sample3.out/part-r-00001
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom1 211
		// tom3 108
		// tom5 109
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00001
		// tom2 209
		// tom4 103
		// tom6 205
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom5-109
		//
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom4-103
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		/**
		 * 3.2
		 * 
		 * job.setPartitionerClass(MyPartitioner.class);
		 * 
		 * job.setNumReduceTasks(1);
		 */
		//
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 2 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:17 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 54 2018-06-26 16:17 /hadoop/test/sample3.out/part-r-00000
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom1 211
		// tom2 209
		// tom3 108
		// tom4 103
		// tom5 109
		// tom6 205
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom4-103
		// mytestreduce:==========
		// mytestreduce:tom5-109
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		/**
		 * 3.3
		 * 
		 * job.setPartitionerClass(MyPartitioner.class);
		 * 
		 * job.setNumReduceTasks(3);
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 4 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:21 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:21 /hadoop/test/sample3.out/part-r-00000
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:21 /hadoop/test/sample3.out/part-r-00001
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:21 /hadoop/test/sample3.out/part-r-00002
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom2 209
		// tom5 109
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00001
		// tom3 108
		// tom6 205
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00002
		// tom1 211
		// tom4 103
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom5-109
		//
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom4-103
		/**
		 * 4.1
		 * 
		 * job.setGroupingComparatorClass(MyGroupingComparator.class);
		 * 
		 * job.setNumReduceTasks(2);
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 3 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:24 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 9 2018-06-26 16:24 /hadoop/test/sample3.out/part-r-00000
		// -rw-r--r-- 2 hadoop supergroup 9 2018-06-26 16:24 /hadoop/test/sample3.out/part-r-00001
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom5 428
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00001
		// tom6 517
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:tom3-108
		// mytestreduce:tom5-109
		//
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:tom4-103
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		/**
		 * 4.2
		 * 
		 * job.setGroupingComparatorClass(MyGroupingComparator.class);
		 * 
		 * job.setNumReduceTasks(1);
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 2 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:28 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 54 2018-06-26 16:28 /hadoop/test/sample3.out/part-r-00000
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom1 211
		// tom2 209
		// tom3 108
		// tom4 103
		// tom5 109
		// tom6 205
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom4-103
		// mytestreduce:==========
		// mytestreduce:tom5-109
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		/**
		 * 4.3
		 * 
		 * job.setGroupingComparatorClass(MyGroupingComparator.class);
		 * 
		 * job.setNumReduceTasks(3);
		 */
		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample3.out
		// Found 4 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 16:31 /hadoop/test/sample3.out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:31 /hadoop/test/sample3.out/part-r-00000
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:31 /hadoop/test/sample3.out/part-r-00001
		// -rw-r--r-- 2 hadoop supergroup 18 2018-06-26 16:31 /hadoop/test/sample3.out/part-r-00002
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00000
		// tom2 209
		// tom5 109
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00001
		// tom3 108
		// tom6 205
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample3.out/part-r-00002
		// tom1 211
		// tom4 103
		// [hadoop@auth-sit bigdata]$
		//
		// mytestreduce:==========
		// mytestreduce:tom2-107
		// mytestreduce:tom2-102
		// mytestreduce:==========
		// mytestreduce:tom5-109
		//
		// mytestreduce:==========
		// mytestreduce:tom3-108
		// mytestreduce:==========
		// mytestreduce:tom6-104
		// mytestreduce:tom6-101
		//
		// mytestreduce:==========
		// mytestreduce:tom1-106
		// mytestreduce:tom1-105
		// mytestreduce:==========
		// mytestreduce:tom4-103

		job.waitForCompletion(true);
	}
}
