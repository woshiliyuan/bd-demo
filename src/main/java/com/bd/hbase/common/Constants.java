package com.bd.hbase.common;

/**
 * @author yuan.li
 *
 */
public class Constants {

	/**
	 * table member
	 *
	 */
	public class tm {
		public final static String table = "member";

		public final static String f_info = "info";

		public class f_info_columns {
			public final static String firstname = "firstname";
			public final static String secondname = "secondname";
			public final static String sex = "sex";
			public final static String age = "age";
		}

		public final static String f_addr = "addr";

		public class f_addr_columns {
			public final static String country = "country";
			public final static String city = "city";
			public final static String addr = "addr";
		}
	}

	/**
	 * table cust
	 *
	 */
	public class tc {
		public final static String table = "cust";

		public final static String f_info = "info";

		public class f_info_columns {
			public final static String firstname = "firstname";
			public final static String secondname = "secondname";
			public final static String sex = "sex";
			public final static String age = "age";
		}

		public final static String f_addr = "addr";

		public class f_addr_columns {
			public final static String country = "country";
			public final static String city = "city";
			public final static String addr = "addr";
		}
	}

	/**
	 * 系统变量字符串
	 *
	 */
	public class params {

		public final static String rowKey = "rowKey";
		public final static String family = "family";

		public final static String startColumn = "startColumn";// get
		public final static String endColumn = "endColumn";// get

		public final static String minStamp = "minStamp";// get or scan
		public final static String maxStamp = "maxStamp";// get or scan
		public final static String versions = "versions";// get or scan

		public final static String startRow = "startRow";// scan
		public final static String stopRow = "stopRow";// scan
		public final static String limit = "limit";// scan

	}
}
