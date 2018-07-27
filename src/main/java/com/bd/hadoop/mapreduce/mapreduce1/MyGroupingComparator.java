package com.bd.hadoop.mapreduce.mapreduce1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author yuan.li
 * 
 *         进入同一个reduce的key是按照顺序排好的，
 * 
 *         该类使得： 如果连续（注意，一定连续）的两条或多条记录满足同组（即compare方法返回0）的条件，
 * 
 *         即使key不相同，他们的value也会进入同一个values,执行一个reduce方法。
 * 
 *         相反，如果原来key相同，但是并不满足同组的条件，他们的value也不会进入一个valeus。
 * 
 *         最后返回的key是：满足这些条件的一组key中排在最后的那个。
 * 
 *         注意：配合setNumReduceTasks使用
 *
 */
public class MyGroupingComparator extends WritableComparator {

	protected MyGroupingComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		Text k1 = (Text) a;
		Text k2 = (Text) b;
		return k1.hashCode() % 2 - k2.hashCode() % 2;
	}
}
