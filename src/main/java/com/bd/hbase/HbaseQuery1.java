package com.bd.hbase;

import java.io.IOException;
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

import com.bd.common.BdException;
import com.bd.hbase.common.Constants;
import com.bd.hbase.common.HbaseExceptionCode;
import com.bd.hbase.common.QualifierModel;

/**
 * get
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
public class HbaseQuery1 extends HBaseCommon {
	/**
	 * @param tableName
	 * @param params
	 *            过滤条件
	 * @return Map<family, List<QualifierModel>>
	 */
	public Map<String, List<QualifierModel>> query(String tableName,
			String rowKey, String family, Map<String, String> params,
			String... qualifiers) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));

			Get get = getGet(rowKey, family, params, qualifiers);
			Result result = table.get(get);

			if (result == null || result.listCells() == null) {
				return null;
			}
			Map<String, List<QualifierModel>> map = new HashMap<String, List<QualifierModel>>();
			for (Cell cell : result.listCells()) {
				String family2 = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier2 = Bytes.toString(CellUtil
						.cloneQualifier(cell));
				String value2 = Bytes.toString(CellUtil.cloneValue(cell));
				Long timestamp2 = cell.getTimestamp();

				QualifierModel qualifierModel = new QualifierModel();
				qualifierModel.setQualifier(qualifier2);
				qualifierModel.setValue(value2);
				qualifierModel.setTimestamp(timestamp2);

				if (map.get(family2) == null) {
					map.put(family2, new ArrayList<QualifierModel>() {
						private static final long serialVersionUID = 1L;
						{
							add(qualifierModel);
						}
					});
				} else {
					map.get(family2).add(qualifierModel);
				}
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

	private Get getGet(String rowKey, String family,
			Map<String, String> params, String... qualifiers)
			throws IllegalArgumentException, IOException {
		Get get = new Get(Bytes.toBytes(rowKey));

		if (StringUtils.isNotEmpty(family)) {
			get.addFamily(Bytes.toBytes(family));// family
			for (String qualifier : qualifiers) {
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));// qualifier
			}
		}
		if (params != null) {
			String minStamp = params.get(Constants.params.minStamp);
			String maxStamp = params.get(Constants.params.maxStamp);
			if (StringUtils.isNotEmpty(minStamp)
					&& StringUtils.isNotEmpty(maxStamp)) {
				get.setTimeRange(Long.valueOf(minStamp), Long.valueOf(maxStamp));// [minStamp
																					// maxStamp)
			}

			List<Filter> filterList = new ArrayList<Filter>();

			String startColumn = params.get(Constants.params.startColumn);
			String endColumn = params.get(Constants.params.endColumn);
			if (StringUtils.isNotEmpty(startColumn)) {
				filterList.add(new QualifierFilter(
						CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(
								Bytes.toBytes(startColumn))));
			}
			if (StringUtils.isNotEmpty(endColumn)) {
				filterList.add(new QualifierFilter(
						CompareOperator.LESS_OR_EQUAL, new BinaryComparator(
								Bytes.toBytes(endColumn))));
			}

			if (filterList.size() > 0) {
				get.setFilter(new FilterList(filterList));
			}
		}
		return get;
	}
}
