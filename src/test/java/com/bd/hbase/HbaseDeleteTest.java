package com.bd.hbase;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HbaseDeleteTest {
	@Autowired
	private HBaseUtils hBaseUtils;

	@Test
	public void test() {// done
		// put 'member', 'aobama2', 'info:firstname', 'bama'
		// put 'member', 'aobama2', 'info:secondname', 'ao'

		// aobama2 column=info:firstname, timestamp=1527230562936, value=bama
		// aobama2 column=info:secondname, timestamp=1527230566975, value=ao

		// deleteall 'member','aobama2'
		hBaseUtils.delete.deleteColumn("member", "aobama2", "info", "firstname");
		hBaseUtils.delete.deleteColumn("member", "aobama2", "info", "firstname2");// 不存在的列

		// aobama2 column=info:secondname, timestamp=1527230566975, value=ao
	}

	@Test
	public void test1() {// done
		// put 'member', 'aobama3', 'info:firstname', 'bama3'
		// put 'member', 'aobama3', 'info:secondname', 'ao3'
		// put 'member', 'aobama3', 'info:age', '18'

		// aobama3 column=info:age, timestamp=1527230983039, value=18
		// aobama3 column=info:firstname, timestamp=1527230898163, value=bama3
		// aobama3 column=info:secondname, timestamp=1527230906860, value=ao3
		List<String> columns = new ArrayList<String>();
		columns.add("firstname");
		columns.add("secondname");
		hBaseUtils.delete.deleteColumn("member", "aobama3", "info", columns);

		// aobama3 column=info:age, timestamp=1527230983039, value=18
	}

	@Test
	public void test2() {// done
		// put 'member', 'aobama4', 'info:firstname', 'bama4'
		// put 'member', 'aobama4', 'info:secondname', 'ao4'

		// aobama4 column=info:firstname, timestamp=1527231334176, value=bama4
		// aobama4 column=info:secondname, timestamp=1527231340477, value=ao4
		hBaseUtils.delete.deleteColumn("member", "aobama4", "info", new ArrayList<String>());

		//
	}

	// 删除数据
	// a )删除行中的某个列值
	//
	// # 语法：delete <table>, <rowkey>, <family:column> , <timestamp>,必须指定列名
	// # 例如：删除表t1，rowkey001中的f1:col1的数据
	// hbase(main)> delete 't1','rowkey001','f1:col1'
	// 注：将删除改行f1:col1列所有版本的数据
	// b )删除行
	//
	// # 语法：deleteall <table>, <rowkey>, <family:column> , <timestamp>，可以不指定列名，删除整行数据
	// # 例如：删除表t1，rowk001的数据
	// hbase(main)> deleteall 't1','rowkey001'
	// c）删除表中的所有数据
	//
	// # 语法： truncate <table>
	// # 其具体过程是：disable table -> drop table -> create table
	// # 例如：删除表t1的所有数据
	// hbase(main)> truncate 't1'
}
