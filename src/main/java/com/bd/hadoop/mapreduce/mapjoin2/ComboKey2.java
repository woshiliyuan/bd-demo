package com.bd.hadoop.mapreduce.mapjoin2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * join端连接 自定义key
 * 
 * @author yuan.li
 *
 */
public class ComboKey2 implements WritableComparable<ComboKey2> {
	// 0-customer 1-order
	private int type;
	private int cid;

	private int oid;
	private String customerInfo = "";
	private String orderInfo = "";

	public int compareTo(ComboKey2 o) {
		int type0 = o.type;
		int cid0 = o.cid;
		int oid0 = o.oid;
		// String customerInfo0 = o.customerInfo;
		// String orderInfo0 = o.orderInfo;

		if (cid == cid0) { // 是否同一个customer的数据
			if (type == type0) { // 同一个客户的两个订单
				return oid - oid0;
			} else {// 一个Customer + 他的order
				if (type == 0)
					return -1;
				else
					return 1;
			}
		}
		// cid不同
		else {
			return cid - cid0;
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		out.writeInt(cid);
		out.writeInt(oid);
		out.writeUTF(customerInfo);
		out.writeUTF(orderInfo);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = in.readInt();
		this.cid = in.readInt();
		this.oid = in.readInt();
		this.customerInfo = in.readUTF();
		this.orderInfo = in.readUTF();
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getCid() {
		return cid;
	}

	public void setCid(int cid) {
		this.cid = cid;
	}

	public int getOid() {
		return oid;
	}

	public void setOid(int oid) {
		this.oid = oid;
	}

	public String getCustomerInfo() {
		return customerInfo;
	}

	public void setCustomerInfo(String customerInfo) {
		this.customerInfo = customerInfo;
	}

	public String getOrderInfo() {
		return orderInfo;
	}

	public void setOrderInfo(String orderInfo) {
		this.orderInfo = orderInfo;
	}
}
