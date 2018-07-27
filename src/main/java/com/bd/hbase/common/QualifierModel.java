package com.bd.hbase.common;

/**
 * @author yuan.li
 *
 */
public class QualifierModel {
	private String qualifier;
	private String value;
	private Long timestamp;

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "QualifierModel [qualifier=" + qualifier + ", value=" + value + ", timestamp=" + timestamp + "]";
	}
}
