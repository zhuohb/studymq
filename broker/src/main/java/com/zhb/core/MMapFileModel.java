package com.zhb.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapFileModel {
	private File file;
	private MappedByteBuffer mappedByteBuffer;
	private FileChannel fileChannel;

	/**
	 * 文件的内存映射
	 *
	 * @param filePath    文件路径
	 * @param startOffset 文件开始映射的offset
	 * @param mappedSize  映射的体积
	 * @throws IOException
	 */
	public void loadFileInMMap(String filePath, int startOffset, int mappedSize) throws IOException {
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("filePath is " + filePath + " inValid");
		}
		fileChannel = new RandomAccessFile(file, "rw").getChannel();
		mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
	}

	/**
	 * 从指定的位置开始读取内容
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

	/**
	 * 清理内存映射
	 *
	 * @throws IOException
	 */
	public void clean() throws IOException {
		// 强制将缓冲区的内容写入到磁盘
		mappedByteBuffer.force();

		mappedByteBuffer.clear();
		fileChannel.close();
	}

}
