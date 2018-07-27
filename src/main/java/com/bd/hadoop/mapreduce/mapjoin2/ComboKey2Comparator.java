package com.bd.hadoop.mapreduce.mapjoin2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * join端连接 组合Key排序对比器
 * 
 * @author yuan.li
 *
 */
public class ComboKey2Comparator extends WritableComparator {
	protected ComboKey2Comparator() {
		super(ComboKey2.class, true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		ComboKey2 k1 = (ComboKey2) a;
		ComboKey2 k2 = (ComboKey2) b;
		return k1.compareTo(k2);
	}
}