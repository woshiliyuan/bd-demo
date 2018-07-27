package com.bd.hbase.common;

/**
 * @author yuan.li
 *
 */
public enum HbaseExceptionCode {

	SERVER_ERROR("0001", "服务异常"),

	HBASE_OPERA_ERROR("0100", "Hbase操作报错");

	private final String errorCode;
	private final String message;

	private HbaseExceptionCode(String errorCode, String message) {
		this.errorCode = errorCode;
		this.message = message;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public String getMessage() {
		return message;
	}
}
