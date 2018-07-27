package com.bd.hadoop.mapreduce.mapjoin2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * join端连接 创建Mapper
 * 
 * @author yuan.li
 *
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, ComboKey2, NullWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		// 判断是customer还是order
		FileSplit split = (FileSplit) context.getInputSplit();
		String path = split.getPath().toString();
		// 客户信息
		ComboKey2 key2 = new ComboKey2();
		if (path.contains("customers")) {
			String cid = line.substring(0, line.indexOf(","));
			String custInfo = line;
			key2.setType(0);
			key2.setCid(Integer.parseInt(cid));
			key2.setCustomerInfo(custInfo);
		}
		// order info
		else if (path.contains("orders")) {
			String cid = line.substring(line.lastIndexOf(",") + 1);
			String oid = line.substring(0, line.indexOf(","));
			String oinfo = line.substring(0, line.lastIndexOf(","));
			key2.setType(1);
			key2.setCid(Integer.parseInt(cid));
			key2.setOid(Integer.parseInt(oid));
			key2.setOrderInfo(oinfo);
		}
		context.write(key2, NullWritable.get());
	}
}