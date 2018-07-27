package com.bd.hadoop.mapreduce.mapjoin2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * join端连接 自定义分区类
 * 
 * @author yuan.li
 *
 */
public class CIDPartitioner extends Partitioner<ComboKey2, NullWritable> {

	public int getPartition(ComboKey2 key, NullWritable nullWritable, int numPartitions) {
		return key.getCid() % numPartitions;
	}
}
