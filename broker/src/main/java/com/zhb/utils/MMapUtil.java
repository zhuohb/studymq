package com.zhb.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 内存映射工具类
 */
public class MMapUtil {

	private File file;
	private MappedByteBuffer mappedByteBuffer;
	private FileChannel fileChannel;

	/**
	 * 指定offset做文件的映射
	 *
	 * @param filePath    文件路径
	 * @param startOffset 开始映射的offset
	 * @param mappedSize  映射的体积
	 */
	public void loadFileInMMap(String filePath, int startOffset, int mappedSize) throws IOException {
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("filePath is " + filePath + " inValid");
		}
		//RandomAccessFile是一个用于读取和写入文件的类,可以随机访问文件的任何部分
		fileChannel = new RandomAccessFile(file, "rw").getChannel();
		//映射文件到内存 映射区域可读可写
		mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
	}

	/**
	 * 支持从文件的指定offset开始读取内容
	 *
	 * @param readOffset
	 * @param size
	 * @return
	 */
	public byte[] readContent(int readOffset, int size) {
		//设置下一个读取或写入操作的起始位置
		mappedByteBuffer.position(readOffset);
		byte[] content = new byte[size];
		// 直接从 mappedByteBuffer 读取 size 个字节到 content 数组中
		mappedByteBuffer.get(content);
		return content;
	}

	/**
	 * 写入数据到磁盘当中
	 * 默认不强制刷盘
	 *
	 * @param content
	 */
	public void writeContent(byte[] content) {
		this.writeContent(content, false);
	}

	/**
	 * 写入数据到磁盘当中
	 *
	 * @param content
	 * @param force
	 */
	public void writeContent(byte[] content, boolean force) {
		//默认刷到page cache中，
		mappedByteBuffer.put(content);
		if (force) {
			//强制刷盘
			mappedByteBuffer.force();
		}
	}

	public void clear() {
		if (mappedByteBuffer != null) {
			mappedByteBuffer.clear();
		}
		if (fileChannel != null) {
			try {
				fileChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
