package com.zhb.utils;


import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;

/**
 * @Author idea
 * @Date: Created in 08:04 2024/3/28
 * @Description
 */
public class CommitLogFileNameUtil {

    /**
     * 构建第一份commitLog文件名称
     * @return
     */
    public static String buildFirstCommitLogName() {
        return "00000000";
    }


    /**
     * 构建新的commitLog文件路径
     *
     * @param topicName
     * @param commitLogFileName
     * @return
     */
    public static String buildCommitLogFilePath(String topicName,String commitLogFileName) {
        return CommonCache.getGlobalProperties().getEagleMqHome()
                + BrokerConstants.BASE_STORE_PATH
                + topicName
                + "/"
                + commitLogFileName;
    }

    /**
     * 根据老的commitLog文件名生成新的commitLog文件名
     * @param oldFileName
     * @return
     */
    public static String incrCommitLogFileName(String oldFileName) {
        if(oldFileName.length() != 8) {
            throw new IllegalArgumentException("fileName must has 8 chars");
        }
        Long fileIndex = Long.valueOf(oldFileName);
        fileIndex++;
        String newFileName = String.valueOf(fileIndex);
        int newFileNameLen = newFileName.length();
        int needFullLen = 8 - newFileNameLen;
        if(needFullLen < 0) {
            throw new RuntimeException("unKnow fileName error");
        }
        StringBuffer stb = new StringBuffer();
        for(int i =0;i<needFullLen;i++) {
            stb.append("0");
        }
        stb.append(newFileName);
        return stb.toString();
    }

    public static void main(String[] args) {
        String newFileName = CommitLogFileNameUtil.incrCommitLogFileName("00000011");
        System.out.println(newFileName);
    }
}
