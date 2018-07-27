package com.bd.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

/**
 * @author yuan.li
 *
 */
public class HiveUtils {

	private Connection connection;
	private PreparedStatement ps;
	// private ResultSet rs;

	private String driver;
	private String url;
	private String user;
	private String passwd;

	@PostConstruct
	public void init() {
		try {
			Class.forName(driver);
			connection = DriverManager.getConnection(url, user, passwd);
			System.out.println(connection);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 执行sql
	public boolean execute(String paramString) {
		boolean result = false;
		try {
			ps = connection.prepareStatement(paramString);
			result = ps.execute(paramString);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close();
		}
		return result;
	}

	// 执行sql
	public List<Map<String, Object>> executeQuery(String paramString) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> rowData = null;

		ResultSet result = null;
		try {
			ps = connection.prepareStatement(paramString);
			result = ps.executeQuery(paramString);
			ResultSetMetaData md = result.getMetaData();
			int size = result.getMetaData().getColumnCount();
			while (result.next()) {
				rowData = new HashMap<String, Object>();
				for (int i = 1; i <= size; i++) {
					rowData.put(md.getColumnName(i), result.getObject(i));
				}
				list.add(rowData);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close();
		}
		return list.size() > 0 ? list : null;
	}

	// 关闭连接
	public void close() {
		// try {
		// if (rs != null) {
		// rs.close();
		// }
		// if (ps != null) {
		// ps.close();
		// }
		// if (connection != null) {
		// connection.close();
		// }
		// } catch (SQLException e) {
		// e.printStackTrace();
		// }
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
}
