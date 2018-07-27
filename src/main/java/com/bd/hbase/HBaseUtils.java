package com.bd.hbase;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.bd.common.BdException;
import com.bd.hbase.common.HbaseExceptionCode;

/**
 * 连接类
 * 
 * @author yuan.li
 *
 */
public class HBaseUtils implements InitializingBean {

	private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

	private static String HBASE_THREADS_MAX = "hbase.htable.threads.max";

	private static String HBASE_THREADS_CORE = "hbase.htable.threads.core";

	private static String HBASE_THREADS_KLTIME = "hbase.htable.threads.keepalivetime";

	private String hbaseZookeeperQuorum;
	private String zookeeperZnodeParent;

	private Configuration configuration;
	private Connection conn;

	/**
	 * 1.如果抛：HADOOP_HOME or hadoop.home.dir are not set. 先配置客户端环境变量：HADOOP_HOME=D:\Program Files(x86)\hadoop-common-2.6.0-bin-master
	 */
	@PostConstruct
	public void init() {
		configuration = HBaseConfiguration.create();
		configuration.set(HConstants.ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
		configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperZnodeParent);

		configuration.set(HBASE_THREADS_MAX, "500");
		configuration.set(HBASE_THREADS_CORE, "50");
		configuration.set(HBASE_THREADS_KLTIME, "20");
		try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			logger.error("[Hbase] -createConnection  error!", e);
			throw new BdException(HbaseExceptionCode.SERVER_ERROR.getErrorCode(), HbaseExceptionCode.SERVER_ERROR.getMessage());
		}
	}

	public HbaseAdmin admin;
	public HbaseInsert insert;
	public HbaseDelete delete;
	public HbaseUpdate update;
	public HbaseQuery query;
	public HbaseQuery1 query1;
	public HbaseQuery2 query2;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (HBaseCommon.conn == null) {
			HBaseCommon.conn = conn;
		}
		admin = new HbaseAdmin();
		insert = new HbaseInsert();
		delete = new HbaseDelete();
		update = new HbaseUpdate();
		query = new HbaseQuery();
		query1 = new HbaseQuery1();
		query2 = new HbaseQuery2();
	}

	public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
	}

	public void setZookeeperZnodeParent(String zookeeperZnodeParent) {
		this.zookeeperZnodeParent = zookeeperZnodeParent;
	}

}
