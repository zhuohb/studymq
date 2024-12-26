package com.zhb.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

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


	public static void main(String[] args) throws IOException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
		Scanner input = new Scanner(System.in);
		int i = 0;
		long size = input.nextLong();
		MMapUtil mMapUtil = new MMapUtil();
		//默认是字节
		mMapUtil.loadFileInMMap("D:\\code\\zhuohb\\backend\\studymq\\broker\\src\\main\\java\\com\\zhb\\config\\store\\order_cancel_topic\\00000000",
			0, (int) (1024 * 1024 * size));
		System.out.println("映射了" + size + "m的空间");
		TimeUnit.SECONDS.sleep(5);
		System.out.println("释放内存");
		mMapUtil.clean();
		TimeUnit.SECONDS.sleep(5);
	}
}
