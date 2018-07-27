package com.bd.hbase;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * @author yuan.li
 *
 */
public class HbaseUpdate extends HBaseCommon {

	/**
	 * 为表的一列数据进行累计功能
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 *            列
	 * @param value
	 *            要增加的计数值,值为负值的时候就是做减法运算
	 * @return
	 */
	public Long incrementColumn(String tableName, String rowKey, String family,
			String column, long value) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Long backValue = table.incrementColumnValue(rowKey.getBytes(),
					family.getBytes(), column.getBytes(), value);
			return backValue;
		} catch (Exception e) {
			logger.error("[Hbase] operate increment column error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 
	 * 批量累计功能,为表的多列都进行累计
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param columns
	 *            要操作的列
	 * @param value
	 *            要增加的计数值,值为负值的时候就是做减法运算
	 */
	public void incrementColumn(String tableName, String rowKey, String family,
			List<String> columns, long value) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Increment increment = new Increment(rowKey.getBytes());
			for (int i = 0; i < columns.size(); i++) {
				increment.addColumn(family.getBytes(), columns.get(i)
						.getBytes(), value);
			}
			table.increment(increment);
		} catch (Exception e) {
			logger.error("[Hbase] batch operate increment column error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}
}
