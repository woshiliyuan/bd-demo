package com.bd.hadoop.mapreduce.mapreduce1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yuan.li
 * 
 *         对key取hash值(或其它处理)，进入不同的reduce
 * 
 *         注意：配合setNumReduceTasks使用，job.setNumReduceTasks(numPartitions)
 */
public class MyPartitioner extends Partitioner<Text, Text> {

	public int getPartition(Text key, Text value, int numPartitions) {
		return key.hashCode() % numPartitions;
	}
}
