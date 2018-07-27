package com.bd.common;

/**
 * @author yuan.li
 *
 */
public class BdException extends RuntimeException {

	private static final long serialVersionUID = 8136282319218571410L;

	protected final String errorCode;

	public String getErrorCode() {
		return errorCode;
	}

	public BdException(String message, String errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public BdException(String message, String errorCode, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}
}
