package com.bd.hadoop.mapreduce.mapreduce1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author yuan.li
 * 
 *         对进入同一个reduce的键或键的一部分进行排序
 */
public class MySortComparator extends WritableComparator {
	protected MySortComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		Text k1 = (Text) a;
		Text k2 = (Text) b;
		return k2.compareTo(k1);
	}
}