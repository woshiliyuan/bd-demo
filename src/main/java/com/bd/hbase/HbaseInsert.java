package com.bd.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * @author yuan.li
 *
 */
public class HbaseInsert extends HBaseCommon {

	/**
	 * 插入一列一行值
	 * 
	 * @param tableName
	 *            表名称，必须存在
	 * @param rowKey
	 *            一条记录的主键
	 * @param family
	 *            列簇，必须存在
	 * @param column
	 *            列簇下某列
	 * @param value
	 *            列簇下某列列值
	 */
	public void putColumn(String tableName, String rowKey, String family,
			String column, String value) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Put put = new Put(rowKey.getBytes());
			put.addColumn(family.getBytes(), column.getBytes(),
					value.getBytes());
			table.put(put);
		} catch (Exception e) {
			logger.error("[Hbase] putColumn error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 批量插入一列簇多列值
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param map
	 *            列：列值
	 */
	public void putColumn(String tableName, String rowKey, String family,
			Map<String, String> map) {

		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Put put = new Put(rowKey.getBytes());

			for (Map.Entry<String, String> entry : map.entrySet()) {

				if (entry.getKey() != null && entry.getValue() != null) {
					put.addColumn(family.getBytes(), entry.getKey().getBytes(),
							entry.getValue().getBytes());
				} else {
					logger.warn(
							"[Hbase] putColumn table:[{}],key:[{}] introduction sets param is null,param:[{}]!",
							tableName, rowKey, entry);
				}
			}
			table.put(put);
		} catch (Exception e) {
			logger.error(
					"[Hbase] putColumn error:{}!,tableName:{},rowKey:{},family:{},map:{}",
					e, tableName, rowKey, family, map);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);// 对性能影响很小
		}
	}

	/**
	 * 批量插入多列簇多列值
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param maps
	 *            Map<列簇, Map<列, 列值>>
	 */
	public void putColumn(String tableName, String rowKey,
			Map<String, Map<String, String>> maps) {
		Table table = null;
		try {
			Put put = new Put(rowKey.getBytes());
			for (Entry<String, Map<String, String>> et : maps.entrySet()) {
				if (et.getKey() != null && et.getValue() != null) {
					for (Entry<String, String> entry : et.getValue().entrySet()) {
						if (entry.getKey() != null && entry.getValue() != null) {
							put.addColumn(et.getKey().getBytes(), entry
									.getKey().getBytes(), entry.getValue()
									.getBytes());
						} else {
							logger.warn(
									"[Hbase] putColumn update table:[{}],key:[{}],introduction sets param is null,param[{}]!",
									tableName, rowKey, entry);
						}
					}
				}
			}
			table = conn.getTable(TableName.valueOf(tableName));
			table.put(put);
		} catch (Exception e) {
			logger.error("[Hbase] putColumn error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 批量插入一列簇多row key,多列值
	 *
	 * @param tableName
	 * @param family
	 * @param map
	 *            Map<rowKey, Map<列, 列值>>
	 * 
	 *            性能优化： @Deprecated
	 * 
	 *            htable.setWriteBufferSize(10 * 1024 * 1024);
	 * 
	 *            htable.setAutoFlush(false);
	 * 
	 *            put.setWriteToWAL(false);
	 * 
	 */
	public void putColumns(String tableName, String family,
			Map<String, Map<String, String>> map) {

		Table table = null;
		List<Put> puts = new ArrayList<Put>();
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			for (Entry<String, Map<String, String>> et : map.entrySet()) {
				Put put = new Put(et.getKey().getBytes());
				for (Entry<String, String> entry : et.getValue().entrySet()) {
					put.addColumn(family.getBytes(), entry.getKey().getBytes(),
							entry.getValue().getBytes());

				}
				puts.add(put);
			}

			table.put(puts);
		} catch (Exception e) {
			logger.error("[Hbase] putColumns error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}
}
