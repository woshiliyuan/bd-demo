package com.bd.hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * @author yuan.li
 *
 */
public class HBaseCommon {
	public static Logger logger = LoggerFactory.getLogger(HBaseCommon.class);

	protected static Connection conn;

	protected void closeTable(Table table) {
		try {
			if (null != table) {
				table.close();
			}
		} catch (Exception e) {
			logger.error("[Hbase] closeTable error!", e);
			throw new BdException(HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(), HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		}
	}

	protected void closeAdmin(Admin admin) {
		try {
			if (null != admin) {
				admin.close();
			}
		} catch (Exception e) {
			logger.error("[Hbase] closeAdmin error!", e);
			throw new BdException(HbaseExceptionCode.HBASE_OPERA_ERROR.getErrorCode(), HbaseExceptionCode.HBASE_OPERA_ERROR.getMessage());
		}
	}

	// /**
	// * 获取Long值
	// *
	// * @param bb
	// * @return
	// */
	// public static long getLong(byte[] bb) {
	// return ((((long) bb[0] & 0xff) << 56) | (((long) bb[1] & 0xff) << 48)
	// | (((long) bb[2] & 0xff) << 40) | (((long) bb[3] & 0xff) << 32)
	// | (((long) bb[4] & 0xff) << 24) | (((long) bb[5] & 0xff) << 16)
	// | (((long) bb[6] & 0xff) << 8) | (((long) bb[7] & 0xff) << 0));
	// }
}
