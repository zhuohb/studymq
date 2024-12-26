package com.zhb.core;

import java.io.IOException;

public class MessageAppendHandler {
	private MMapFileModelManager mMapFileModelManager = new MMapFileModelManager();

	/**
	 * 创建一个model并映射文件,并将model放入管理器中
	 *
	 * @throws IOException
	 */
	public void prepareMMapLoading(String filePath, String topicName) throws IOException {
		MMapFileModel mapFileModel = new MMapFileModel();
		mapFileModel.loadFileInMMap(filePath, 0, 1 * 1024 * 1024);
		mMapFileModelManager.put(topicName, mapFileModel);
	}

	public void appendMsg(String topic, String content) {
		MMapFileModel mapFileModel = mMapFileModelManager.get(topic);
		if (mapFileModel == null) {
			throw new RuntimeException("topic is invalid!");
		}
		mapFileModel.writeContent(content.getBytes());
	}

	public void readMsg(String topic) {
		MMapFileModel mapFileModel = mMapFileModelManager.get(topic);
		if (mapFileModel == null) {
			throw new RuntimeException("topic is invalid!");
		}
		byte[] content = mapFileModel.readContent(0, 10);
		System.out.println(new String(content));
	}
}
