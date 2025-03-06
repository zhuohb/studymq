package com.zhb.broker.utils;

import com.zhb.broker.cache.CommonCache;
import com.zhb.common.constants.BrokerConstants;

/**
 * @Author idea
 * @Date: Created in 08:04 2024/3/28
 * @Description
 */
public class LogFileNameUtil {

    /**
     * 构建第一份commitLog文件名称
     *
     * @return
     */
    public static String buildFirstCommitLogName() {
        return "00000000";
    }

    /**
     * 构建第一份consumeQueue文件名称
     *
     * @return
     */
    public static String buildFirstConsumeQueueName() {
        return "00000000";
    }


    /**
     * 构建commitLog文件路径
     *
     * @param topicName
     * @param commitLogFileName
     * @return
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
     *
     * @param topicName
     * @return
     */
    public static String buildCommitLogBasePath(String topicName) {
        return CommonCache.getGlobalProperties().getEagleMqHome()
                + BrokerConstants.BASE_COMMIT_PATH
                + topicName;
    }

    /**
     * 构建consumeQueue的文件路径
     *
     * @param topicName
     * @param queueId
     * @param consumeQueueFileName
     * @return
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
     * 构建consumeQueue的文件路径
     *
     * @param topicName
     * @return
     */
    public static String buildConsumeQueueBasePath(String topicName) {
        return CommonCache.getGlobalProperties().getEagleMqHome()
                + BrokerConstants.BASE_CONSUME_QUEUE_PATH
                + topicName;
    }



    public static String incrConsumeQueueFileName(String oldFileName) {
        return incrCommitLogFileName(oldFileName);
    }

    /**
     * 根据老的commitLog文件名生成新的commitLog文件名
     *
     * @param oldFileName
     * @return
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

    public static void main(String[] args) {
        String newFileName = LogFileNameUtil.incrCommitLogFileName("00000011");
        System.out.println(newFileName);
    }
}
