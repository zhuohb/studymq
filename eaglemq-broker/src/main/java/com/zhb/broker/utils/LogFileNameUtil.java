package com.zhb.broker.utils;

import com.zhb.broker.cache.CommonCache;
import com.zhb.common.constants.BrokerConstants;

/**
 * 日志文件名称工具类
 * 提供生成和管理消息存储相关文件名称的工具方法
 * 包括commitLog和consumeQueue文件的命名和路径构建
 */
public class LogFileNameUtil {

	/**
	 * 构建第一份commitLog文件名称
	 * 使用固定格式"00000000"作为初始文件名
	 *
	 * @return 初始commitLog文件名
	 */
	public static String buildFirstCommitLogName() {
		return "00000000";
	}

	/**
	 * 构建第一份consumeQueue文件名称
	 * 使用固定格式"00000000"作为初始文件名
	 *
	 * @return 初始consumeQueue文件名
	 */
	public static String buildFirstConsumeQueueName() {
		return "00000000";
	}


	/**
	 * 构建commitLog文件完整路径
	 * 基于全局配置的主目录、主题名称和文件名构建
	 *
	 * @param topicName         主题名称
	 * @param commitLogFileName commitLog文件名
	 * @return commitLog文件的完整路径
	 */
	public static String buildCommitLogFilePath(String topicName, String commitLogFileName) {
		return CommonCache.getGlobalProperties().getEagleMqHome()
			+ BrokerConstants.BASE_COMMIT_PATH
			+ topicName
			+ BrokerConstants.SPLIT
			+ commitLogFileName;
	}


	/**
	 * 构建commitLog所在目录的基本路径
	 * 不包含具体的文件名，仅到主题目录
	 *
	 * @param topicName 主题名称
	 * @return commitLog目录的基础路径
	 */
	public static String buildCommitLogBasePath(String topicName) {
		return CommonCache.getGlobalProperties().getEagleMqHome()
			+ BrokerConstants.BASE_COMMIT_PATH
			+ topicName;
	}

	/**
	 * 构建consumeQueue的文件完整路径
	 * 基于全局配置的主目录、主题名称、队列ID和文件名构建
	 *
	 * @param topicName            主题名称
	 * @param queueId              队列ID
	 * @param consumeQueueFileName consumeQueue文件名
	 * @return consumeQueue文件的完整路径
	 */
	public static String buildConsumeQueueFilePath(String topicName, Integer queueId, String consumeQueueFileName) {
		return CommonCache.getGlobalProperties().getEagleMqHome()
			+ BrokerConstants.BASE_CONSUME_QUEUE_PATH
			+ topicName
			+ BrokerConstants.SPLIT
			+ queueId
			+ BrokerConstants.SPLIT
			+ consumeQueueFileName;
	}

	/**
	 * 构建consumeQueue的基本路径
	 * 不包含队列ID和文件名，仅到主题目录
	 *
	 * @param topicName 主题名称
	 * @return consumeQueue目录的基础路径
	 */
	public static String buildConsumeQueueBasePath(String topicName) {
		return CommonCache.getGlobalProperties().getEagleMqHome()
			+ BrokerConstants.BASE_CONSUME_QUEUE_PATH
			+ topicName;
	}


	/**
	 * 生成递增的consumeQueue文件名
	 * 与commitLog文件名增长逻辑相同
	 *
	 * @param oldFileName 当前文件名
	 * @return 递增后的新文件名
	 */
	public static String incrConsumeQueueFileName(String oldFileName) {
		return incrCommitLogFileName(oldFileName);
	}

	/**
	 * 根据老的commitLog文件名生成新的commitLog文件名
	 * 文件名格式为8位数字，如"00000001"
	 * 每次递增时保持前导零，确保文件名长度不变
	 *
	 * @param oldFileName 当前文件名
	 * @return 递增后的新文件名
	 * @throws IllegalArgumentException 如果文件名长度不是8位
	 * @throws RuntimeException         如果递增后超出8位数字范围
	 */
	public static String incrCommitLogFileName(String oldFileName) {
		if (oldFileName.length() != 8) {
			throw new IllegalArgumentException("fileName must has 8 chars");
		}
		Long fileIndex = Long.valueOf(oldFileName);
		fileIndex++;
		String newFileName = String.valueOf(fileIndex);
		int newFileNameLen = newFileName.length();
		int needFullLen = 8 - newFileNameLen;
		if (needFullLen < 0) {
			throw new RuntimeException("unKnow fileName error");
		}
		StringBuffer stb = new StringBuffer();
		for (int i = 0; i < needFullLen; i++) {
			stb.append("0");
		}
		stb.append(newFileName);
		return stb.toString();
	}

	/**
	 * 测试方法
	 * 演示递增commitLog文件名的功能
	 */
	public static void main(String[] args) {
		String newFileName = LogFileNameUtil.incrCommitLogFileName("00000011");
		System.out.println(newFileName);
	}
}
