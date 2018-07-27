package com.bd.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * 日志分析-统计每天出现ip次数最多的前3条
 * 
 * @author yuan.li
 *
 */
public class LogAnalysis {

	public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String regexDate = "[0-9]{4}[-][0-9]{1,2}[-][0-9]{1,2}";
			String regexIp = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
			Pattern pDate = Pattern.compile(regexDate);
			Pattern pIp = Pattern.compile(regexIp);

			String text = value.toString();
			Matcher mDate = pDate.matcher(text);
			Matcher mIp = pIp.matcher(text);

			String date, ip;

			boolean resultDate = mDate.find();
			if (resultDate) {
				date = mDate.group(0);
				boolean resultIp = mIp.find();
				while (resultIp) {
					ip = mIp.group();
					context.write(new Text(date), new Text(ip));
					resultIp = mIp.find();
				}
			}
		}
	}

	public static class LogReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> ipMap = new HashMap<String, Integer>();

			for (Text value : values) {
				String ip = value.toString();
				Integer cnt = ipMap.get(ip);
				if (cnt != null)
					ipMap.put(ip, cnt + 1);
				else
					ipMap.put(ip, 1);
			}

			List<Map.Entry<String, Integer>> iplist = new ArrayList<Map.Entry<String, Integer>>(ipMap.entrySet());
			Collections.sort(iplist, new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}

			});

			String res = "";
			int cnt = 0;
			for (Map.Entry<String, Integer> m : iplist) {
				if (cnt == 3)
					break;
				res += m.getKey() + "\t" + m.getValue().toString() + "\t";
				cnt += 1;
			}
			result.set(res);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {// done
		String inputPaths = "/hadoop/test/hadoop-hadoop-namenode-auth-sit.log";
		String outputPath = "/hadoop/test/hadoop-hadoop-namenode-auth-sit_loganalysis";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Log Analysis");
		job.setJarByClass(LogAnalysis.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(Text.class);//
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPaths));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/hadoop-hadoop-namenode-auth-sit_loganalysis/part-r-00000
		// 2018-05-23 172.29.2.174 496 172.29.2.175 210 172.29.2.173 44
		// 2018-05-24 172.29.2.174 1735 172.29.2.175 1017 172.29.2.173 186
		// 2018-05-25 172.29.2.174 1510 172.29.2.175 790 172.29.2.173 140
		// 2018-05-26 172.29.2.174 1418 172.29.2.175 699 172.29.2.173 120
		// 2018-05-27 172.29.2.174 1420 172.29.2.175 702 172.29.2.173 120
		// 2018-05-28 172.29.2.174 1095 172.29.2.175 532 172.29.2.173 95
		// [hadoop@auth-sit bigdata]$
	}

	// 19:06:41.344 [main] INFO org.apache.hadoop.mapreduce.Job - Counters: 38
	// File System Counters
	// FILE: Number of bytes read=645354
	// FILE: Number of bytes written=2324710
	// FILE: Number of read operations=0
	// FILE: Number of large read operations=0
	// FILE: Number of write operations=0
	// HDFS: Number of bytes read=39988706
	// HDFS: Number of bytes written=382
	// HDFS: Number of read operations=33
	// HDFS: Number of large read operations=0
	// HDFS: Number of write operations=6
	// Map-Reduce Framework
	// Map input records=70559
	// Map output records=12329
	// Map output bytes=295896
	// Map output materialized bytes=320572
	// Input split bytes=399
	// Combine input records=0
	// Combine output records=0
	// Reduce input groups=6
	// Reduce shuffle bytes=320572
	// Reduce input records=12329
	// Reduce output records=6
	// Spilled Records=24658
	// Shuffled Maps =3
	// Failed Shuffles=0
	// Merged Map outputs=3
	// GC time elapsed (ms)=36
	// CPU time spent (ms)=0
	// Physical memory (bytes) snapshot=0
	// Virtual memory (bytes) snapshot=0
	// Total committed heap usage (bytes)=1648361472
	// Shuffle Errors
	// BAD_ID=0
	// CONNECTION=0
	// IO_ERROR=0
	// WRONG_LENGTH=0
	// WRONG_MAP=0
	// WRONG_REDUCE=0
	// File Input Format Counters
	// Bytes Read=11933425
	// File Output Format Counters
	// Bytes Written=382
}
