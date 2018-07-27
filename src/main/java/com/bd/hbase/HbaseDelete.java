package com.bd.hbase;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * @author yuan.li
 *
 */
public class HbaseDelete extends HBaseCommon {

	/**
	 * 删除某一行的一列列值
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 *            不指定column时，删除rowKey
	 */
	public void deleteColumn(String tableName, String rowKey, String family, String column) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column)); // 不指定column时，删除rowKey
			table.delete(delete);
		} catch (Exception e) {
			logger.error("[Hbase] deleteColumn error!", e);
			throw new BdException(HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(), HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 批量删除某一行的多列值
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param columns
	 *            不指定column时，删除rowKey
	 */
	public void deleteColumn(String tableName, String rowKey, String family, List<String> columns) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			for (int i = 0; i < columns.size(); i++) {
				delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(columns.get(i)));
			}
			table.delete(delete);
		} catch (Exception e) {
			logger.error("[Hbase] deleteColumn error!", e);
			throw new BdException(HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(), HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}
}
