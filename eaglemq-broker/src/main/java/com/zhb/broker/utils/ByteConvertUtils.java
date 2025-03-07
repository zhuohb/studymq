package com.zhb.broker.utils;

/**
 * 字节转换工具类
 * 提供字节数组和整数之间的相互转换功能
 * 支持从字节数组中读取指定位置的数据、整数到字节数组的转换等操作
 */
public class ByteConvertUtils {

	/**
	 * 从源字节数组的指定位置读取指定长度的数据
	 *
	 * @param source 源字节数组
	 * @param pos    起始位置
	 * @param len    需要读取的长度
	 * @return 读取到的新字节数组
	 */
	public static byte[] readInPos(byte[] source, int pos, int len) {
		byte[] result = new byte[len];
		for (int i = pos, j = 0; i < pos + len; i++) {
			result[j++] = source[i];
		}
		return result;
	}

	/**
	 * 将整数转换为字节数组（小端序）
	 * 一个整数转换为4个字节，低位在前，高位���后
	 *
	 * @param value 需要转换的整数值
	 * @return 转换后的4字节数组
	 */
	public static byte[] intToBytes(int value) {
		byte[] src = new byte[4];
		// 按位与操作确保每个字节只保留8位
		// 右移操作提取整数的不同部分
		src[3] = (byte) ((value >> 24) & 0xFF); // 取最高8位
		src[2] = (byte) ((value >> 16) & 0xFF); // 取次高8位
		src[1] = (byte) ((value >> 8) & 0xFF);  // 取次低8位
		src[0] = (byte) (value & 0xFF);         // 取最低8位
		return src;
	}

	/**
	 * 将字节数组转换为整数（小端序）
	 * 将4个字节重组为一个整数，低位在前，高位在后
	 *
	 * @param ary 源字节数组，长度应为4
	 * @return 转换后的整数值
	 */
	public static int bytesToInt(byte[] ary) {
		int value;
		// 通过位运算将4个字节重组为整数
		// 每个字节先进行与操作保证是无符号8位值，再左移对应位置
		value = (int) ((ary[0] & 0xFF)
			| ((ary[1] << 8) & 0xFF00)
			| ((ary[2] << 16) & 0xFF0000)
			| ((ary[3] << 24) & 0xFF000000));
		return value;
	}

	/**
	 * 测试方法
	 * 演示整数与字节数组之间的互相转换
	 */
	public static void main(String[] args) {
		int j = 100;
		// 将整数100转换为字节数组
		byte[] byteContent = ByteConvertUtils.intToBytes(j);
		System.out.println(byteContent.length);
		// 再将字节数组转换回整数
		int result = ByteConvertUtils.bytesToInt(byteContent);
		System.out.println(result);
	}
}
