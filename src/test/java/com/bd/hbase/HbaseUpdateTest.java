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
public class HbaseUpdateTest {
	@Autowired
	private HBaseUtils hBaseUtils;

	@Test
	public void test() {// done
		// 在第一次使用时计数器（实质为列）隐藏的进行了创建
		// incr 'member', 'liyuan3', 'info:age', 1
		long cnt1 = hBaseUtils.update.incrementColumn("member", "liyuan3", "info", "age", 18);
		long cnt2 = hBaseUtils.update.incrementColumn("member", "liyuan3", "info", "age", 3);
		long cnt3 = hBaseUtils.update.incrementColumn("member", "liyuan4", "info", "age", 18);
		long cnt4 = hBaseUtils.update.incrementColumn("member", "liyuan4", "info", "age", -3);
		long cnt5 = hBaseUtils.update.incrementColumn("member", "liyuan5", "info", "age", 18);
		System.out.println(cnt1);
		System.out.println(cnt2);
		System.out.println(cnt3);
		System.out.println(cnt4);
		System.out.println(cnt5);

		// liyuan3 column=info:age, timestamp=1527228033636, value=\x00\x00\x00\x00\x00\x00\x00\x15
		// liyuan4 column=info:age, timestamp=1527228033667, value=\x00\x00\x00\x00\x00\x00\x00\x0F
		// liyuan5 column=info:age, timestamp=1527228033681, value=\x00\x00\x00\x00\x00\x00\x00\x12
	}

	@Test
	public void test1() {// done
		List<String> columns = new ArrayList<String>();
		columns.add("age");
		columns.add("age1");
		columns.add("age2");
		hBaseUtils.update.incrementColumn("member", "liyuan6", "info", columns, 18);
		hBaseUtils.update.incrementColumn("member", "liyuan6", "info", columns, 4);
		hBaseUtils.update.incrementColumn("member", "liyuan6", "info", columns, -2);
		// liyuan6 column=info:age, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
		// liyuan6 column=info:age1, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
		// liyuan6 column=info:age2, timestamp=1527228471270, value=\x00\x00\x00\x00\x00\x00\x00\x14
	}
}
