package com.zhb.broker.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.CountDownLatch;

/**
 * 内存映射文件工具类
 * 支持基于Java的MMap API访问文件的能力
 * 提供高性能的文件读写操作
 *
 * 主要功能：
 * - 支持指定偏移量的文件映射（映射大小=结束偏移量-开始偏移量）
 * - 支持从指定偏移量开始读取文件内容
 * - 支持从指定偏移量开始写入文件内容
 * - 支持映射内存资源释放
 */
public class MMapUtil {

	/**
	 * 映射的文件对象
	 */
	private File file;

	/**
	 * 内存映射缓冲区
	 * 注意：不受JVM GC自动管理
	 */
	private MappedByteBuffer mappedByteBuffer;

	/**
	 * 原子偏移量
	 */
	private Long atomicOffset;

	/**
	 * 文件通道
	 */
	private FileChannel fileChannel;

	/**
	 * 在指定偏移量处映射文件到内存
	 *
	 * @param filePath    文件路径
	 * @param startOffset 开始映射的偏移量
	 * @param mappedSize  映射的大小（字节）
	 * @throws IOException 如果文件不存在或映射失败
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
	 * 从映射内存的指定偏移量读取内容
	 *
	 * @param readOffset 读取起始偏移量
	 * @param size 读取大小
	 * @return 读取的字节数组
	 */
	public byte[] readContent(int readOffset, int size) {
		mappedByteBuffer.position(readOffset);
		byte[] content = new byte[size];
		int j = 0;
		for (int i = 0; i < size; i++) {
			// 从内存映射空间中读取数据
			byte b = mappedByteBuffer.get(readOffset + i);
			content[j++] = b;
		}
		return content;
	}

	/**
	 * 写入内容到映射内存
	 * 默认不强制刷盘
	 *
	 * @param content 要写入的字节数组
	 */
	public void writeContent(byte[] content) {
		this.writeContent(content, false);
	}

	/**
	 * 写入内容到映射内存
	 * 可选是否强制刷盘
	 *
	 * @param content 要写入的字节数组
	 * @param force 是否强制刷盘，true表示立即写入物理磁盘
	 */
	public void writeContent(byte[] content, boolean force) {
		// 默认写入到操作系统页缓存中
		mappedByteBuffer.put(content);
		if (force) {
			// 强制刷盘：确保数据写入物理磁盘
			mappedByteBuffer.force();
		}
	}

	/**
	 * 清理映射的内存资源（不推荐使用）
	 *
	 * @deprecated 不推荐的原因是因为使用了sun包下不稳定的代码
	 */
	public void clear() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		// 在关闭资源时执行以下代码释放内存
		// 不推荐的原因是因为使用了sun包下不稳定的代码
		// Method m = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
		// m.setAccessible(true);
		// m.invoke(FileChannelImpl.class, mappedByteBuffer);
	}

	/**
	 * 清理DirectBuffer资源
	 * 通过反射调用cleaner方法释放映射的内存资源
	 */
	public void clean() {
		if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() || mappedByteBuffer.capacity() == 0)
			return;
		invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
	}

	/**
	 * 通过反射调用目标对象的指定方法
	 * 使用特权操作执行，避免安全管理器限制
	 *
	 * @param target 目标对象
	 * @param methodName 方法名
	 * @param args 方法参数类型
	 * @return 方法执行结果
	 */
	private Object invoke(final Object target, final String methodName, final Class<?>... args) {
		return AccessController.doPrivileged(new PrivilegedAction<Object>() {
			public Object run() {
				try {
					Method method = method(target, methodName, args);
					method.setAccessible(true);
					return method.invoke(target);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
	}

	/**
	 * 获取目标对象的指定方法
	 * 先尝试公共方法，再尝试声明的方法
	 *
	 * @param target 目标对象
	 * @param methodName 方法名
	 * @param args 方法参数类型
	 * @return 找到的方法对象
	 * @throws NoSuchMethodException 如果找不到对应方法
	 */
	private Method method(Object target, String methodName, Class<?>[] args)
		throws NoSuchMethodException {
		try {
			return target.getClass().getMethod(methodName, args);
		} catch (NoSuchMethodException e) {
			return target.getClass().getDeclaredMethod(methodName, args);
		}
	}

	/**
	 * 获取ByteBuffer的视图缓冲区
	 * 用于获取DirectBuffer的原始缓冲区
	 *
	 * @param buffer 输入缓冲区
	 * @return 视图缓冲区
	 */
	private ByteBuffer viewed(ByteBuffer buffer) {
		String methodName = "viewedBuffer";
		Method[] methods = buffer.getClass().getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals("attachment")) {
				methodName = "attachment";
				break;
			}
		}

		ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
		if (viewedBuffer == null)
			return buffer;
		else
			return viewed(viewedBuffer);
	}

	/**
	 * 测试方法
	 * 演示并发环境下MMapUtil的使用
	 */
	public static void main(String[] args) throws IOException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
		MMapUtil mMapUtil = new MMapUtil();
		// 映射1MB大小的文件内容到内存
		mMapUtil.loadFileInMMap("/Users/linhao/IdeaProjects-new/eaglemq/broker/store/order_cancel_topic/00000000", 0, 1024 * 1024 * 1);
		CountDownLatch count = new CountDownLatch(1);
		CountDownLatch allWriteSuccess = new CountDownLatch(10);

		// 创建10个线程并发写入
		for(int i = 0; i < 10; i++) {
			int finalI = i;
			Thread task = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						count.await();
						// 多线程并发写入测试
						mMapUtil.writeContent(("test-content-" + finalI).getBytes());
						allWriteSuccess.countDown();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			});
			task.start();
		}

		System.out.println("准备执行并发写入mmap测试");
		count.countDown();
		allWriteSuccess.await();
		System.out.println("并发测试完毕,读取文件内容测试");

		// 读取写入的内容
		byte[] content = mMapUtil.readContent(0, 1000);
		System.out.println("内容：" + new String(content));
	}
}
