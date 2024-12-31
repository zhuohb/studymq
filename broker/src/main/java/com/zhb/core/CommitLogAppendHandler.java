package com.zhb.core;

import com.zhb.constants.BrokerConstants;
import com.zhb.model.CommitLogMessageModel;

import java.io.IOException;

/**
 * CommitLog文件指的是00000000这一类的文件
 */
public class CommitLogAppendHandler {

    private MMapFileModelManager mMapFileModelManager = new MMapFileModelManager();

    /**
     * 加载
     * @param topicName
     * @throws IOException
     */
    public void prepareMMapLoading(String topicName) throws IOException {
        MMapFileModel mapFileModel = new MMapFileModel();
        mapFileModel.loadFileInMMap(topicName, 0, BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE);
        mMapFileModelManager.put(topicName, mapFileModel);
    }


    public void appendMsg(String topic, byte[] content) throws IOException {
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
        byte[] content = mapFileModel.readContent(0, 1000);
        System.out.println(new String(content));
    }

}
