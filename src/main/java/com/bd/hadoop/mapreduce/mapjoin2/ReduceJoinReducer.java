package com.bd.hadoop.mapreduce.mapjoin2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * join端连接 reducer端连接实现。
 * 
 * @author yuan.li
 */
public class ReduceJoinReducer extends Reducer<ComboKey2, NullWritable, Text, NullWritable> {

	protected void reduce(ComboKey2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<NullWritable> it = values.iterator();
		String cinfo = key.getCustomerInfo();
		while (it.hasNext()) {
			it.next();
			String oinfo = key.getOrderInfo();
			context.write(new Text(cinfo + "," + oinfo), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPaths = "/hadoop/test/customers.txt,/hadoop/test/orders.txt";
		String outputPath = "/hadoop/test/customers_out";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapJoinMapper");

		// 搜索类
		job.setJarByClass(ReduceJoinMapper.class); 
		// 添加输入路径
		FileInputFormat.addInputPaths(job, inputPaths);
		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(ReduceJoinMapper.class); // mapper类
		job.setReducerClass(ReduceJoinReducer.class); // reducer类
		// 设置Map输出类型
		job.setMapOutputKeyClass(ComboKey2.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 设置ReduceOutput类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 设置分区类
		job.setPartitionerClass(CIDPartitioner.class);
		// 设置分组对比器
		job.setGroupingComparatorClass(CIDGroupComparator.class);
		// reduce个数
		job.setNumReduceTasks(2);
		// 设置排序对比器
		job.setSortComparatorClass(ComboKey2Comparator.class);
		job.waitForCompletion(true);

		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/customers_out/
		// Found 3 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-06-26 09:26 /hadoop/test/customers_out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 89 2018-06-26 09:26 /hadoop/test/customers_out/part-r-00000
		// -rw-r--r-- 2 hadoop supergroup 158 2018-06-26 09:26 /hadoop/test/customers_out/part-r-00001
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/customers_out/part-r-00000
		// 2,tom,13,
		// 2,tom,13,3,no001,12.23
		// 2,tom,13,4,no001,12.23
		// 2,tom,13,5,no001,12.23
		// 4,tom,15,
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/customers_out/part-r-00001
		// 1,tom,12,
		// 1,tom,12,1,no001,12.23
		// 1,tom,12,2,no001,12.23
		// 3,tom,14,
		// 3,tom,14,6,no001,12.23
		// 3,tom,14,7,no001,12.23
		// 3,tom,14,8,no001,12.23
		// 3,tom,14,9,no001,12.23
		// [hadoop@auth-sit bigdata]$
	}
}
