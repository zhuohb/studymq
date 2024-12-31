package com.zhb.core;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MMapFileModelManager {

    /**
     * key:主题名称，value:文件的mMap对象
     */
    private Map<String,MMapFileModel> mMapFileModelMap = new HashMap<>();

    public void put(String topic,MMapFileModel mapFileModel) {
        mMapFileModelMap.put(topic,mapFileModel);
    }

    public MMapFileModel get(String topic) {
        return mMapFileModelMap.get(topic);
    }

}
