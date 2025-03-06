package com.zhb.broker.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 10:35 2024/4/21
 * @Description consumequeue的mmap映射对象的管理器
 */
public class ConsumeQueueMMapFileModelManager {

    public Map<String, List<ConsumeQueueMMapFileModel>> consumeQueueMMapFileModel = new HashMap<>();

    public void put(String topic,List<ConsumeQueueMMapFileModel> consumeQueueMMapFileModels) {
        consumeQueueMMapFileModel.put(topic,consumeQueueMMapFileModels);
    }

    public List<ConsumeQueueMMapFileModel> get(String topic) {
        return consumeQueueMMapFileModel.get(topic);
    }
}
