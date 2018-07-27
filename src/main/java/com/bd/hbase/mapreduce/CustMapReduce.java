package com.bd.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 使用 HBase 作为 MapReduce 源，并使用一个总结步骤。此示例将计算表中某个值的不同实例的数量，并将这些汇总计数写入另一个HBase表中。
 * 
 * @author yuan.li
 *
 */
public class CustMapReduce {

	public static class MyTableMapper extends TableMapper<Text, IntWritable> {
		public static final byte[] CF = "info".getBytes();
		public static final byte[] ATTR1 = "firstname".getBytes();

		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			String val = new String(value.getValue(CF, ATTR1));
			text.set(val); // we can only emit Writables...
			context.write(text, ONE);
		}
	}

	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		public static final byte[] CF = "info".getBytes();
		public static final byte[] COUNT = "count".getBytes();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(CF, COUNT, Bytes.toBytes(i));

			context.write(null, put);
		}
	}
}
