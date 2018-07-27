package com.bd.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hadoop.hdfs.HdfsService;
import com.bd.hadoop.mapreduce.LogCleaner.MyMapper;
import com.bd.hadoop.mapreduce.LogCleaner.MyReducer;
import com.bd.hadoop.mapreduce.LogParser;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class LogCleanerTest {
	@Autowired
	private HdfsService hdfsService;

	private String inputPaths = "/hadoop/test/sample2.txt";
	private String outputPath = "/hadoop/test/sample2_out";

	private String localfile = "D:/project/test/bd-demo/src/test/java/com/bd/hadoop/sample2.txt";

	@Before
	public void setUp() throws Exception {
		hdfsService.deleteFile(inputPaths);
		hdfsService.deleteFile(outputPath);

		hdfsService.uploadFile(inputPaths, localfile);
	}

	@Test
	public void test() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {// done
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(LogParser.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPaths));
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);

		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test/sample2_out
		// Found 2 items
		// -rw-r--r-- 2 hadoop supergroup 0 2018-05-29 16:49 /hadoop/test/sample2_out/_SUCCESS
		// -rw-r--r-- 2 hadoop supergroup 417 2018-05-29 16:49 /hadoop/test/sample2_out/part-r-00000
		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample2_out/part-r-00000
		// 27.19.74.143 20130430173820 image/common/faq.gif HTTP/1.1\
		// 27.19.74.143 20130430173820 image/common/faq.gif HTTP/1.1\
		// 27.19.74.143 20130430173820 image/common/faq.gif HTTP/1.1\
		// 27.19.74.143 20130430173820 image/common/faq.gif HTTP/1.1\
		// 27.19.74.144 20130430173820 GET image/common/faq.gif HTTP/1.1\
		// 27.19.74.145 20140430173820 image/common/faq.gif HTTP/1.1\
		// 27.19.74.146 20140430173820 image/common/faq.gif HTTP/1.1\
		// [hadoop@auth-sit bigdata]$
	}
}
