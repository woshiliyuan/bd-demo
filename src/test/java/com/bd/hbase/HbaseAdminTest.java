package com.bd.hbase;

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
public class HbaseAdminTest {

	@Autowired
	private HBaseUtils hBaseUtils;

	@Test
	public void test() {// done
		// create 'yuan', {NAME => 'f1', VERSIONS => '1'}
		hBaseUtils.admin.createTable("yuan111", "f1");
	}

	@Test
	public void test1() {// done
		// exists 'yuan'
		System.out.println(hBaseUtils.admin.tableExists("yuan"));
		System.out.println(hBaseUtils.admin.tableExists("yuan11"));
	}

	@Test
	public void test2() {// done
		// create 'yuan2', {NAME => 'info1,info2', VERSIONS => '1'}
		hBaseUtils.admin.createTable("yuan2222", "f1", "f2");
	}

	@Test
	public void test3() {// done
		// create 'member', {NAME => 'info,addr', VERSIONS => '1'}
		// hBaseUtils.admin.deleteTable("member");
		hBaseUtils.admin.createTable("member", "info", "addr");
	}

	@Test
	public void test4() {// done
		// drop 'yuan'
		hBaseUtils.admin.deleteTable("yuan");
		hBaseUtils.admin.deleteTable("yuan2");
	}
	/**
	 * 笔记：
	 * 
	 * 1.因为hbase是面向表+列簇，所以我们只需要建立表+列簇，无需指定列簇里的属性名称
	 * 
	 */

	// 表的管理
	// 1）查看有哪些表
	// hbase(main)> list
	// 2）创建表
	//
	// # 语法：create <table>, {NAME => <family>, VERSIONS => <VERSIONS>}
	// # 例如：创建表t1，有两个family name：f1，f2，且版本数均为2
	// hbase(main)> create 't1',{NAME => 'f1', VERSIONS => 2},{NAME => 'f2',
	// VERSIONS => 2}
	// 3）删除表
	// 分两步：首先disable，然后drop
	// 例如：删除表t1
	//
	// hbase(main)> disable 't1'
	// hbase(main)> drop 't1'
	// 4）查看表的结构
	//
	// # 语法：describe <table>
	// # 例如：查看表t1的结构
	// hbase(main)> describe 't1'
	// 5）修改表结构
	// 修改表结构必须先disable
	//
	// # 语法：alter 't1', {NAME => 'f1'}, {NAME => 'f2', METHOD => 'delete'}
	// # 例如：修改表test1的cf的TTL为180天
	// hbase(main)> disable 'test1'
	// hbase(main)> alter 'test1',{NAME=>'body',TTL=>'15552000'},{NAME=>'meta',
	// TTL=>'15552000'}
	// hbase(main)> enable 'test1'
	// 权限管理
	// 1）分配权限
	// # 语法 : grant <user> <permissions> <table> <column family> <column
	// qualifier> 参数后面用逗号分隔
	// # 权限用五个字母表示： "RWXCA".
	// # READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A')
	// # 例如，给用户‘test'分配对表t1有读写的权限，
	// hbase(main)> grant 'test','RW','t1'
	// 2）查看权限
	//
	// # 语法：user_permission <table>
	// # 例如，查看表t1的权限列表
	// hbase(main)> user_permission 't1'
	// 3）收回权限
	//
	// # 与分配权限类似，语法：revoke <user> <table> <column family> <column qualifier>
	// # 例如，收回test用户在表t1上的权限
	// hbase(main)> revoke 'test','t1'

	// hbase中的常见属性
	//
	// VERSIONS：指版本数
	//
	// MIN_VERSIONS=> '0'：最小版本数
	//
	// TTL=> 'FOREVER'：版本存活时间
	//
	// 假设versions=10,mini_version=4
	//
	// 到达TTL时间后，version-mini_version=6，最老的6个版本的值会被清空
	//
	//
	//
	// create't2', {NAME => 'f1', VERSIONS => 1000,MIN_VERSIONS => '1000',TTL
	// =>'31536000'}
	//
	//
	//
	// BLOOMFILTER=> 'ROW':布隆过滤器
	//
	// -》NONE：不使用布隆过滤器
	//
	// -》ROW：行级布隆过滤器
	//
	// -》ROWCOL：行列布隆过滤器
	//
	// 进行storefile文件检索的时候：
	//
	// ROW：会对当前的storefile进行判断，判断是否有我需要的rowkey，
	//
	// 如果有就读，没有就跳过
	//
	// ROWCOL：会对当前的storefile进行判断，判断是否有我需要的rowkey+列标签的组合，
	//
	// 如果有就读，没有就跳过
	//
	// 这种消耗的资源较大
	//
	// BLOCKSIZE=> '65536'：数据块的大小，如果你的数据块越小，索引就越大
	//
	// 占用的内存就越高，速度会更快
	//
	// create't3', {NAME => 'f1', BLOCKSIZE => '65536'}
	//
	// BLOCKCACHE=> 'true'：缓存，默认就是true
	//
	// 建议：在企业中，对于不常使用的列簇，关闭缓存
	//
	// IN_MEMORY=> 'false'：缓存中的级别，设置成TRUE，将优先缓存该列簇
	//
	// 缓存清理的算法：LRU
}
