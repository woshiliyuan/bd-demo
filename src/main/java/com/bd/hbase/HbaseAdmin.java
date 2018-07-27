package com.bd.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * @author yuan.li
 *
 */
public class HbaseAdmin extends HBaseCommon {

	/**
	 * 创建表
	 * 
	 * @param tableName
	 * @param familys
	 */
	public void createTable(String tableName, String... familys) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder
					.newBuilder(TableName.valueOf(tableName));
			for (String family : familys) {
				tableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder
						.newBuilder(Bytes.toBytes(family)).build());
			}
			admin.createTable(tableDescriptor.build());
		} catch (Exception e) {
			logger.error("[Hbase] createTable error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeAdmin(admin);
		}
	}

	/**
	 * 删除表
	 * 
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			TableName name = TableName.valueOf(tableName);
			admin.disableTable(name);
			admin.deleteTable(name);
		} catch (Exception e) {
			logger.error("[Hbase] deleteTable error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeAdmin(admin);
		}
	}

	/**
	 * 表是否存在
	 * 
	 * @param tableName
	 * @return
	 */
	public Boolean tableExists(String tableName) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			return admin.tableExists(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.error("[Hbase] tableExists error!", e);
			throw new BdException(
					HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(),
					HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		} finally {
			closeAdmin(admin);
		}
	}
}
