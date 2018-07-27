package com.bd.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;

/**
 * @author yuan.li
 *
 */
@Service
public class HdfsService {
	FileSystem fs = null;

	@PostConstruct
	public void init() {
		try {
			// 初始化文件系统
			// fs = FileSystem.get(new URI("hdfs://auth-sit:9000"), new Configuration(), "hadoop");
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 文件上传
	 * 
	 * @param path
	 * @param localfile
	 * @return
	 * @throws IOException
	 */
	public boolean uploadFile(String path, String localfile) {
		try {
			OutputStream os = fs.create(new Path(path));
			FileInputStream fis = new FileInputStream(localfile);
			IOUtils.copyBytes(fis, os, 2048, true);
			// 可以使用hadoop提供的简单方式
			// fs.copyFromLocalFile(new Path(localfile), new Path(path));
			return true;
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 文件下载
	 * 
	 * @param hadfile
	 * @param localPath
	 * @return
	 */
	public boolean downloadFile(String hadfile, String localPath) {
		try {
			InputStream is = fs.open(new Path(hadfile));
			FileOutputStream fos = new FileOutputStream(localPath);
			IOUtils.copyBytes(is, fos, 2048);
			// 可以使用hadoop提供的简单方式
			// fs.copyToLocalFile(new Path(hadfile), new Path(localPath));
			return true;
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 文件删除
	 * 
	 * @param hadfile
	 * @return
	 */
	public boolean deleteFile(String hadfile) {

		try {
			return fs.delete(new Path(hadfile), true);
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	// hadoop常用命令：
	// hdfs dfs 查看Hadoop HDFS支持的所有命令
	// hdfs dfs –ls 列出目录及文件信息
	// hdfs dfs –lsr 循环列出目录、子目录及文件信息
	// hdfs dfs –put test.txt /user/sunlightcs 将本地文件系统的test.txt复制到HDFS文件系统的/user/sunlightcs目录下
	// hdfs dfs –get /user/sunlightcs/test.txt . 将HDFS中的test.txt复制到本地文件系统中，与-put命令相反
	// hdfs dfs –cat /user/sunlightcs/test.txt 查看HDFS文件系统里test.txt的内容
	// hdfs dfs –tail /user/sunlightcs/test.txt 查看最后1KB的内容
	// hdfs dfs –rm /user/sunlightcs/test.txt 从HDFS文件系统删除test.txt文件，rm命令也可以删除空目录
	// hdfs dfs –rmr /user/sunlightcs 删除/user/sunlightcs目录以及所有子目录
	// hdfs dfs –copyFromLocal test.txt /user/sunlightcs/test.txt 从本地文件系统复制文件到HDFS文件系统，等同于put命令
	// hdfs dfs –copyToLocal /user/sunlightcs/test.txt test.txt 从HDFS文件系统复制文件到本地文件系统，等同于get命令
	// hdfs dfs –chgrp [-R] /user/sunlightcs 修改HDFS系统中/user/sunlightcs目录所属群组，选项-R递归执行，跟linux命令一样
	// hdfs dfs –chown [-R] /user/sunlightcs 修改HDFS系统中/user/sunlightcs目录拥有者，选项-R递归执行
	// hdfs dfs –chmod [-R] MODE /user/sunlightcs 修改HDFS系统中/user/sunlightcs目录权限，MODE可以为相应权限的3位数或+/-{rwx}，选项-R递归执行
	// hdfs dfs –count [-q] PATH 查看PATH目录下，子目录数、文件数、文件大小、文件名/目录名
	// hdfs dfs –cp SRC [SRC …] DST 将文件从SRC复制到DST，如果指定了多个SRC，则DST必须为一个目录
	// hdfs dfs –du PATH 显示该目录中每个文件或目录的大小
	// hdfs dfs –dus PATH 类似于du，PATH为目录时，会显示该目录的总大小
	// hdfs dfs –expunge 清空回收站，文件被删除时，它首先会移到临时目录.Trash/中，当超过延迟时间之后，文件才会被永久删除
	// hdfs dfs –getmerge SRC [SRC …] LOCALDST [addnl] 获取由SRC指定的所有文件，将它们合并为单个文件，并写入本地文件系统中的LOCALDST，选项addnl将在每个文件的末尾处加上一个换行符
	// hdfs dfs –touchz PATH 创建长度为0的空文件
	// hdfs dfs –test –[ezd] PATH 对PATH进行如下类型的检查： -e PATH是否存在，如果PATH存在，返回0，否则返回1 -z 文件是否为空，如果长度为0，返回0，否则返回1 -d 是否为目录，如果PATH为目录，返回0，否则返回1
	// hdfs dfs –text PATH 显示文件的内容，当文件为文本文件时，等同于cat，文件为压缩格式（gzip以及hadoop的二进制序列文件格式）时，会先解压缩
	// hdfs dfs –help ls 查看某个[ls]命令的帮助文档
}
