package com.bd.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.CollectionUtils;

import com.bd.common.BdException;
import com.bd.hbase.common.Constants;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * get
 * 
 * @author yuan.li
 *
 *
 *         HBase的查询实现只提供两种方式：
 * 
 *         1.按指定RowKey获取唯一一条记录，get方法（org.apache.hadoop.hbase.client.Get）
 * 
 *         2.按指定的条件获取一批记录，scan方法（org.apache.hadoop.hbase.client.Scan）
 *         实现条件查询功能使用的就是scan方式
 */
public class HbaseQuery extends HBaseCommon {

	/**
	 * 查询rowKey
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public Map<String, String> get(String tableName, String rowKey) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));

			Result result = table.get(get);
			List<Cell> cells = result.listCells();

			Map<String, String> results = new HashMap<String, String>();
			for (Cell cell : cells) {
				byte[] key = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);
				results.put(new String(key), new String(value));
			}
			return results;
		} catch (Exception e) {
			logger.error("[Hbase] get error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 查询rowKey
	 * 
	 * @param tableName
	 * @param family
	 * @param rowKey
	 * @return
	 * 
	 *         如果tableName或者family不存在，抛错
	 * 
	 *         org.apache.hadoop.hbase.TableNotFoundException:
	 * 
	 *         org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException:
	 */
	public Map<String, String> getRow(String tableName, String rowKey,
			String family) {

		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(family));
			Result result = table.get(get);
			List<Cell> cells = result.listCells();
			if (result == null || CollectionUtils.isEmpty(cells)) {
				return null;
			}
			Map<String, String> results = new HashMap<String, String>();
			for (Cell cell : cells) {
				byte[] key = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);
				results.put(new String(key), new String(value));
			}
			return results;
		} catch (Exception e) {
			logger.error("[Hbase] getRow error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 查询column
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 * @return
	 */
	public String getColumn(String tableName, String rowKey, String family,
			String column) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey.getBytes());
			get.addColumn(family.getBytes(), column.getBytes());
			Result result = table.get(get);
			List<Cell> cells = result.listCells();
			String backValue = null;
			if (result != null && !CollectionUtils.isEmpty(cells)) {
				backValue = new String(CellUtil.cloneValue(cells.get(0)));
			}
			return backValue;
		} catch (Exception e) {
			logger.error("[Hbase] getColumn error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 查询范围：列数
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param startColumn
	 * @param startEoual
	 * @param endColumn
	 * @param endEoual
	 * @return
	 */
	public Long getRangeColumnCount(String tableName, String rowKey,
			String family) {
		List<Cell> cells = getCell(tableName, rowKey, family, null);
		return cells != null ? cells.size() : 0l;
	}

	/**
	 * 查询范围：分页
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param startColumn
	 * @param startEoual
	 * @param endColumn
	 * @param endEoual
	 * @return
	 */
	public List<String> getRangeColumn(String tableName, String rowKey,
			String family, Map<String, String> params) {
		List<String> list = new ArrayList<String>();
		List<Cell> cells = getCell(tableName, rowKey, family, params);
		if (CollectionUtils.isEmpty(cells)) {
			return list;
		}
		for (Cell cell : cells) {
			list.add(new String(CellUtil.cloneQualifier(cell)) + ":"
					+ new String(CellUtil.cloneValue(cell)));
		}
		return list;
	}

	/**
	 * 查询某一行按列照范围的合并值
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param startColumn
	 * @param endColumn
	 * @return
	 */
	public Long getMergerValue(String tableName, String rowKey, String family,
			Map<String, String> params) {
		List<Cell> cells = getCell(tableName, rowKey, family, params);
		if (CollectionUtils.isEmpty(cells)) {
			return 0L;
		}
		Long results = 0L;
		for (Cell cell : cells) {
			results += Bytes.toLong(CellUtil.cloneValue(cell));
		}
		return results;
	}

	/**
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param params
	 * @return
	 */
	protected List<Cell> getCell(String tableName, String rowKey,
			String family, Map<String, String> params) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			if (StringUtils.isNotEmpty(family))
				get.addFamily(Bytes.toBytes(family));

			if (params != null) {
				List<Filter> filterList = new ArrayList<Filter>();
				for (Map.Entry<String, String> entry : params.entrySet()) {
					Filter filter = null;
					if (Constants.params.startColumn.equals(entry.getKey())) {
						String startColumn = params
								.get(Constants.params.startColumn);
						if (!StringUtils.isEmpty(startColumn)) {
							filter = new QualifierFilter(
									CompareOperator.GREATER_OR_EQUAL,
									new BinaryComparator(
											Bytes.toBytes(startColumn)));
						}
					}
					if (Constants.params.endColumn.equals(entry.getKey())) {
						String endColumn = params
								.get(Constants.params.endColumn);
						if (!StringUtils.isEmpty(endColumn)) {
							filter = new QualifierFilter(
									CompareOperator.LESS_OR_EQUAL,
									new BinaryComparator(
											Bytes.toBytes(endColumn)));
						}
					}
					if (filter != null) {
						filterList.add(filter);
					}
				}
				if (filterList.size() > 0) {
					FilterList filterLista = new FilterList(filterList);
					get.setFilter(filterLista);
				}

			}
			Result result = table.get(get);
			if (result == null) {
				return null;
			}
			return result.listCells();
		} catch (Exception e) {
			logger.error("[Hbase] getCell error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeTable(table);
		}
	}
}
