package com.bd.hadoop;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.hadoop.hdfs.HdfsService;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class HdfsServiceTest {

	@Autowired
	private HdfsService hdfsService;

	private String path = "/hadoop/test/sample1.txt";
	private String localfile = "D:/project/test/bd-demo/src/test/java/com/bd/hadoop/sample1.txt";

	@Test
	public void uploadFile() {// done
		hdfsService.uploadFile(path, localfile);

		// [hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test
		// Found 1 items
		// -rw-r--r-- 1 hadoop supergroup 533 2018-05-28 17:46 /hadoop/test/sample1.txt
		// [hadoop@auth-sit bigdata]$

		// [hadoop@auth-sit bigdata]$ hadoop fs -cat /hadoop/test/sample1.txt
		// 0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999
		// 0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999
		// 0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999
		// 0043012650999991949032412004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+01111+99999999999
		// 0043012650999991949032418004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+00781+99999999999
		// [hadoop@auth-sit bigdata]$
	}

	@Test
	public void downloadFile() {// done
		hdfsService.downloadFile(path, "D:/project/test/bd-demo/src/test/java/com/bd/hadoop/sample1_down.txt");
	}

	@Test
	public void deleteFile() {// done
		hdfsService.deleteFile(path);

		// hadoop@auth-sit bigdata]$ hadoop fs -ls /hadoop/test
		// [hadoop@auth-sit bigdata]$
	}
}
