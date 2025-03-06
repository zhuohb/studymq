package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created at 2024/7/25
 * @Description
 */
public class ConsumeMsgCommitLogDTO {

    private String fileName;

    private long commitLogOffset;

    private int commitLogSize;

    private byte[] body;

    private int retryTimes;

    public byte[] getBody() {
        return body;
    }

    public int getCommitLogSize() {
        return commitLogSize;
    }

    public void setCommitLogSize(int commitLogSize) {
        this.commitLogSize = commitLogSize;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
}
