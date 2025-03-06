package com.zhb.broker.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 22:50 2024/3/25
 * @Description
 */
public class CommitLogMMapFileModelManager {

    /**
     * key:主题名称，value:文件的mMap对象
     */
    private Map<String, CommitLogMMapFileModel> mMapFileModelMap = new HashMap<>();

    public void put(String topic, CommitLogMMapFileModel mapFileModel) {
        mMapFileModelMap.put(topic,mapFileModel);
    }

    public CommitLogMMapFileModel get(String topic) {
        return mMapFileModelMap.get(topic);
    }

}
