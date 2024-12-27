package com.zhb.utils;

/**
 * 提供在整数和字节数组之间进行转换的方法。
 * 用于数据的序列化和反序列化
 */
public class ByteConvertUtils {

	/**
	 * 将一个整数转换为长度为4的字节数组
	 * 数组中的每个字节代表整数的一部分，最高有效字节在最高索引处
	 *
	 * @param value
	 * @return
	 */
	public static byte[] intToBytes(int value) {
		byte[] src = new byte[4];
		//32位-24位=8位
		//00000000001 0xFF 16
		src[3] = (byte) ((value >> 24) & 0xFF);
		src[2] = (byte) ((value >> 16) & 0xFF);
		src[1] = (byte) ((value >> 8) & 0xFF);
		src[0] = (byte) (value & 0xFF);
		return src;
	}

	/**
	 * 将长度为4的字节数组转换回整数
	 * 通过移位和掩码组合字节以重建原始整数
	 *
	 * @param ary
	 * @return
	 */
	public static int bytesToInt(byte[] ary) {
		int value;
		value = (ary[0] & 0xFF)
			| ((ary[1] << 8) & 0xFF00)
			| ((ary[2] << 16) & 0xFF0000)
			| ((ary[3] << 24) & 0xFF000000);
		return value;
	}

	public static void main(String[] args) {
		int j = 100;
		//4个字节，1byte=8bit, byte[4]
		byte[] byteContent = ByteConvertUtils.intToBytes(j);
		System.out.println(byteContent.length);
		int result = ByteConvertUtils.bytesToInt(byteContent);
		System.out.println(result);
	}
}
