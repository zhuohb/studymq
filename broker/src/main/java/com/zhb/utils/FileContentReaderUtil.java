package com.zhb.utils;

import com.alibaba.fastjson.JSON;
import com.zhb.model.TopicModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

/**
 * 简化版本的文件读取工具
 */
public class FileContentReaderUtil {

	/**
	 * 从指定路径读取文件内容
	 *
	 * @param path
	 * @return
	 */
	public static String readFromFile(String path) {
		try (BufferedReader in = new BufferedReader(new FileReader(path))) {
			StringBuffer buffer = new StringBuffer();
			while (in.ready()) {
				buffer.append(in.readLine());
			}
			return buffer.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {
		String content = FileContentReaderUtil.readFromFile("D:\\code\\zhuohb\\backend\\studymq\\broker\\config\\json\\mq-topic.json");
		System.out.println(content);
		List<TopicModel> modelList = JSON.parseArray(content, TopicModel.class);
		System.out.println(modelList);
	}

}
