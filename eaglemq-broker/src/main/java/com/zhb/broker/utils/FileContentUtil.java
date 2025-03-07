package com.zhb.broker.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 文件内容工具类
 * 提供简化的文件读写操作方法
 * 支持文件内容的读取和覆盖写入功能
 */
public class FileContentUtil {

	/**
	 * 从文件中读取内容
	 * 将文件的所有行读取并拼接成一个字符串返回
	 *
	 * @param path 文件路径
	 * @return 文件内容字符串
	 * @throws RuntimeException 如果文件读取过程中发生异常
	 */
	public static String readFromFile(String path) {
		try (BufferedReader in = new BufferedReader(new FileReader(path))) {
			StringBuffer stb = new StringBuffer();
			while (in.ready()) {
				stb.append(in.readLine());
			}
			return stb.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 覆盖写入文件内容
	 * 将指定字符串内容写入到指定路径的文件中，会覆盖原有内容
	 *
	 * @param path    文件路径
	 * @param content 要写入的内容
	 * @throws RuntimeException 如果文件写入过程中发生异常
	 */
	public static void overWriteToFile(String path, String content) {
		try (FileWriter fileWriter = new FileWriter(path)) {
			fileWriter.write(content);
			fileWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
