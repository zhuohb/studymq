package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.QueueModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author idea
 * @Date: Created in 10:38 2024/4/21
 * @Description
 */
public class ConsumeQueueAppendHandler {

    public void prepareConsumeQueue(String topicName) throws IOException {
        EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topicName);
        List<QueueModel> queueModelList = eagleMqTopicModel.getQueueList();
        //retry重试topic没有consume queue存在
        if(queueModelList == null) {
            return;
        }
        List<ConsumeQueueMMapFileModel> consumeQueueMMapFileModels = new ArrayList<>();
        //循环遍历，mmap的初始化
        for (QueueModel queueModel : queueModelList) {
            ConsumeQueueMMapFileModel consumeQueueMMapFileModel = new ConsumeQueueMMapFileModel();
            consumeQueueMMapFileModel.loadFileInMMap(
                    topicName,
                    queueModel.getId(),
                    queueModel.getLastOffset(),
                    queueModel.getLatestOffset().get(),
                    queueModel.getOffsetLimit());
            consumeQueueMMapFileModels.add(consumeQueueMMapFileModel);
        }
        CommonCache.getConsumeQueueMMapFileModelManager().put(topicName,consumeQueueMMapFileModels);
    }
}
