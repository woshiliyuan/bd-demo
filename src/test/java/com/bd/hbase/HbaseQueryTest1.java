package com.bd.hbase;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hbase.common.QualifierModel;

/**
 * @author yuan.li
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HbaseQueryTest1 {

	@Autowired
	private HBaseUtils hBaseUtils;

	private String tableName = "cust";

	@Test
	public void test() {
		// 准备数据见：data_cust.txt:scan 'cust'
		System.out.println("1.====================");
		String rowKey = "liyuan";
		Map<String, List<QualifierModel>> map = hBaseUtils.query1.query(tableName, rowKey, null, null);
		System.out.println(map);
		System.out.println("2.====================");
		String rowKey2 = "liyuan";
		String family2 = "info";
		Map<String, List<QualifierModel>> map2 = hBaseUtils.query1.query(tableName, rowKey2, family2, null);
		System.out.println(map2);
		// 1.====================
		// {addr=[QualifierModel [qualifier=area, value=pudong, timestamp=1527644514584], QualifierModel [qualifier=city, value=shanghai,
		// timestamp=1527644514568]], info=[QualifierModel [qualifier=firstname, value=li, timestamp=1527644514495], QualifierModel
		// [qualifier=secondname, value=yuan, timestamp=1527644514551]]}
		// 2.====================
		// {info=[QualifierModel [qualifier=firstname, value=li, timestamp=1527644514495], QualifierModel [qualifier=secondname, value=yuan, timestamp=1527644514551]]}
	}
}
