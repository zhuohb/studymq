package com.zhb.broker.core;

import java.util.HashMap;
import java.util.Map;

/**
 * CommitLog内存映射文件模型管理器
 * 负责管理和维护所有主题的CommitLog内存映射文件模型
 * 提供按主题名称存取CommitLog映射文件模型的能力
 */
public class CommitLogMMapFileModelManager {

	/**
	 * 主题与CommitLog映射文件的映射关系
	 * key: 主题名称
	 * value: 对应主题的CommitLog内存映射文件模型
	 */
	private Map<String, CommitLogMMapFileModel> mMapFileModelMap = new HashMap<>();

	/**
	 * 存储指定主题的CommitLog内存映射文件模型
	 *
	 * @param topic        主题名称
	 * @param mapFileModel 对应的CommitLog内存映射文件模型
	 */
	public void put(String topic, CommitLogMMapFileModel mapFileModel) {
		mMapFileModelMap.put(topic, mapFileModel);
	}

	/**
	 * 获取指定主题的CommitLog内存映射文件模型
	 *
	 * @param topic 主题名称
	 * @return 对应的CommitLog内存映射文件模型，如果不存在则返回null
	 */
	public CommitLogMMapFileModel get(String topic) {
		return mMapFileModelMap.get(topic);
	}

}
