package com.zhb.broker.model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author idea
 * @Date: Created in 22:52 2024/3/26
 * @Description
 */
public class QueueModel {

    private Integer id;
    private String fileName;
    private Integer offsetLimit;
    private AtomicInteger latestOffset;
    private Integer lastOffset;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Integer getOffsetLimit() {
        return offsetLimit;
    }

    public void setOffsetLimit(Integer offsetLimit) {
        this.offsetLimit = offsetLimit;
    }

    public AtomicInteger getLatestOffset() {
        return latestOffset;
    }

    public void setLatestOffset(AtomicInteger latestOffset) {
        this.latestOffset = latestOffset;
    }

    public Integer getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(Integer lastOffset) {
        this.lastOffset = lastOffset;
    }

    public int countDiff() {
        return this.getOffsetLimit() - this.getLatestOffset().get();
    }
}
