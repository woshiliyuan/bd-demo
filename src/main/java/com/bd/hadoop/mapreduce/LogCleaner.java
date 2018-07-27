package com.bd.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 日志清洗
 * 
 * @author yuan.li
 *
 */
public class LogCleaner {

	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		LogParser logParser = new LogParser();
		Text v2 = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {

			final String[] parsed = logParser.parse(value.toString());
			try {

				if (parsed == null || parsed.length < 2) {
					return;
				}
				// 过滤掉静态信息
				if (parsed[2].startsWith("GET /static/") || parsed[2].startsWith("GET /uc_server")) {
					return;
				}
				// 过掉开头的特定格式字符串
				if (parsed[2].startsWith("GET /")) {
					parsed[2] = parsed[2].substring("GET /".length());
				} else if (parsed[2].startsWith("POST /")) {
					parsed[2] = parsed[2].substring("POST /".length());
				}
				// 过滤结尾的特定格式字符串
				if (parsed[2].endsWith(" HTTP/1.1")) {
					parsed[2] = parsed[2].substring(0, parsed[2].length() - " HTTP/1.1".length());
				}
				v2.set(parsed[0] + "\t" + parsed[1] + "\t" + parsed[2]);
				context.write(key, v2);
			} catch (Exception e) {
				System.out.println(e);
			}
		}

	}

	public static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1, Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text v2 : arg1) {
				context.write(v2, NullWritable.get());
			}
		}
	}
}
