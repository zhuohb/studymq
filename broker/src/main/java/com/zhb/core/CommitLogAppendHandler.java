package com.zhb.core;

import com.zhb.model.CommitLogMessageModel;

import java.io.IOException;

public class CommitLogAppendHandler {
	private MMapFileModelManager mMapFileModelManager = new MMapFileModelManager();

	/**
	 * 创建一个model并映射文件,并将model放入管理器中
	 *
	 * @throws IOException
	 */
	public void prepareMMapLoading(String topicName) throws IOException {
		MMapFileModel mapFileModel = new MMapFileModel();
		mapFileModel.loadFileInMMap(topicName, 0, 1 * 1024 * 1024);
		mMapFileModelManager.put(topicName, mapFileModel);
	}

	public void appendMsg(String topic, byte[] content) {
		MMapFileModel mapFileModel = mMapFileModelManager.get(topic);
		if (mapFileModel == null) {
			throw new RuntimeException("topic is invalid!");
		}
		CommitLogMessageModel commitLogMessageModel = new CommitLogMessageModel();
		commitLogMessageModel.setSize(content.length);
		commitLogMessageModel.setContent(content);
		mapFileModel.writeContent(commitLogMessageModel);
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
