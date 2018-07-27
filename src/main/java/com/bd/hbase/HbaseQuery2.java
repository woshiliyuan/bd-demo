package com.bd.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.bd.common.BdException;
import com.bd.hbase.common.Constants;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * scan
 * 
 * @author yuan.li
 * 
 *         HBase的查询实现只提供两种方式：
 * 
 *         1.按指定RowKey获取唯一一条记录，get方法（org.apache.hadoop.hbase.client.Get）
 * 
 *         2.按指定的条件获取一批记录，scan方法（org.apache.hadoop.hbase.client.Scan）
 *         实现条件查询功能使用的就是scan方式
 */
public class HbaseQuery2 extends HBaseCommon {

	/**
	 * @param tableName
	 * @param params
	 *            过滤条件
	 * @return Map<rowkey, Map<family:qualifier, value>>
	 */
	public Map<String, Map<String, String>> query(String tableName,
			Map<String, String> params) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));

			Scan scan = getScan(params);
			ResultScanner resultScanner = table.getScanner(scan);
			if (resultScanner == null) {
				return null;
			}

			Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
			for (Result next = resultScanner.next(); next != null; next = resultScanner
					.next()) {
				Map<String, String> map2 = new HashMap<String, String>();

				for (Cell cell : next.listCells()) {// 某个rowkey下的循坏
					String key2 = Bytes.toString(CellUtil.cloneFamily(cell))
							+ ":"
							+ Bytes.toString(CellUtil.cloneQualifier(cell));
					String value2 = Bytes.toString(CellUtil.cloneValue(cell));
					map2.put(key2, value2);
				}
				map.put(new String(next.getRow()), map2);
			}
			return map;
		} catch (IOException | IllegalArgumentException e) {
			logger.error("[Hbase] query error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	private Scan getScan(Map<String, String> params)
			throws IllegalArgumentException, IOException {
		Scan scan = new Scan();

		String family = params.get(Constants.params.family);
		if (StringUtils.isNotEmpty(family)) {
			scan.addFamily(Bytes.toBytes(family));// family
		}
		String minStamp = params.get(Constants.params.minStamp);
		String maxStamp = params.get(Constants.params.maxStamp);
		if (StringUtils.isNotEmpty(minStamp)
				&& StringUtils.isNotEmpty(maxStamp)) {
			scan.setTimeRange(Long.valueOf(minStamp), Long.valueOf(maxStamp));// [minStamp
																				// maxStamp)
		}
		// String limit = params.get(Constants.params.limit);
		// if (StringUtils.isNotEmpty(limit)) {
		// scan.setLimit(Integer.valueOf(limit));// limit
		// }
		String startRow = params.get(Constants.params.startRow);
		String stopRow = params.get(Constants.params.stopRow);
		if (StringUtils.isNotEmpty(startRow)) {
			scan.withStartRow(Bytes.toBytes(startRow));// inclusive
		}
		if (StringUtils.isNotEmpty(stopRow)) {
			scan.withStopRow(Bytes.toBytes(stopRow));// exclusive

		}
		return scan;
	}

	/**
	 * 分页查询
	 * 
	 * @param tableName
	 * @param startRow
	 *            下一页时，要保留上一页的stopRow=startRow
	 * @param stopRow
	 * @param num
	 * @return Map<rowkey, Map<family:qualifier, value>>
	 */
	public Map<String, Map<String, String>> page(String tableName,
			String startRow, String stopRow, Integer pageSize) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));

			Filter filter = new PageFilter(pageSize);// 每页展示条数
			Scan scan = new Scan();
			scan.setFilter(filter);
			if (StringUtils.isNotEmpty(startRow)) {
				scan.withStartRow(Bytes.toBytes(startRow));// [
			}
			if (StringUtils.isNotEmpty(stopRow)) {
				scan.withStopRow(Bytes.toBytes(stopRow));// )
			}
			ResultScanner resultScanner = table.getScanner(scan);
			if (resultScanner == null) {
				return null;
			}

			Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
			Result next = null;
			while ((next = resultScanner.next()) != null) {
				Map<String, String> map2 = new HashMap<String, String>();

				for (Cell cell : next.listCells()) {// 某个rowkey下的循坏
					String key2 = Bytes.toString(CellUtil.cloneFamily(cell))
							+ ":"
							+ Bytes.toString(CellUtil.cloneQualifier(cell));
					String value2 = Bytes.toString(CellUtil.cloneValue(cell));
					map2.put(key2, value2);
				}
				map.put(new String(next.getRow()), map2);
			}
			return map;
		} catch (IOException | IllegalArgumentException e) {
			logger.error("[Hbase] page error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}
}
