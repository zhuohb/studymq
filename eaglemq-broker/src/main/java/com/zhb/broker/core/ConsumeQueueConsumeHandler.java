package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.*;
import com.zhb.broker.model.*;
import com.zhb.broker.utils.AckMessageLock;
import com.zhb.broker.utils.UnfailReentrantLock;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 20:17 2024/4/27
 * @Description 消费队列消费处理器
 */
public class ConsumeQueueConsumeHandler {

    public AckMessageLock ackMessageLock = new UnfailReentrantLock();

    /**
     * 读取当前最新N条consumeQueue的消息内容,并且返回commitLog原始数据
     *
     * @return
     */
    public List<ConsumeMsgCommitLogDTO> consume(ConsumeQueueConsumeReqModel consumeQueueConsumeReqModel) {
        String topic = consumeQueueConsumeReqModel.getTopic();
        //1.检查参数合法性
        //2.获取当前匹配的队列的最新的consumeQueue的offset是多少
        //3.获取当前匹配的队列存储文件的mmap对象，然后读取offset地址的数据
        EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
        if (eagleMqTopicModel == null) {
            throw new RuntimeException("topic " + topic + " not exist!");
        }
        String consumeGroup = consumeQueueConsumeReqModel.getConsumeGroup();
        Integer queueId = consumeQueueConsumeReqModel.getQueueId();
        Integer batchSize = consumeQueueConsumeReqModel.getBatchSize();

        ConsumeQueueOffsetModel.OffsetTable offsetTable = CommonCache.getConsumeQueueOffsetModel().getOffsetTable();
        Map<String, ConsumeQueueOffsetModel.ConsumerGroupDetail> consumerGroupDetailMap = offsetTable.getTopicConsumerGroupDetail();
        ConsumeQueueOffsetModel.ConsumerGroupDetail consumerGroupDetail = consumerGroupDetailMap.get(topic);
        //如果是首次消费
        if (consumerGroupDetail == null) {
            consumerGroupDetail = new ConsumeQueueOffsetModel.ConsumerGroupDetail();
            consumerGroupDetailMap.put(topic, consumerGroupDetail);
        }
        Map<String, Map<String, String>> consumeGroupOffsetMap = consumerGroupDetail.getConsumerGroupDetailMap();
        Map<String, String> queueOffsetDetailMap = consumeGroupOffsetMap.get(consumeGroup);
        List<QueueModel> queueList = eagleMqTopicModel.getQueueList();
        if (queueOffsetDetailMap == null) {
            queueOffsetDetailMap = new HashMap<>();
            for (QueueModel queueModel : queueList) {
                queueOffsetDetailMap.put(String.valueOf(queueModel.getId()), "00000000#0");
            }
            consumeGroupOffsetMap.put(consumeGroup, queueOffsetDetailMap);
        }
        String offsetStrInfo = queueOffsetDetailMap.get(String.valueOf(queueId));
        String[] offsetStrArr = offsetStrInfo.split("#");
        Integer consumeQueueOffset = Integer.valueOf(offsetStrArr[1]);
        QueueModel queueModel = queueList.get(queueId);
        //消费到了尽头
        if (queueModel.getLatestOffset().get() <= consumeQueueOffset) {
            return null;
        }
        List<ConsumeQueueMMapFileModel> consumeQueueOffsetModels = CommonCache.getConsumeQueueMMapFileModelManager().get(topic);
        ConsumeQueueMMapFileModel consumeQueueMMapFileModel = consumeQueueOffsetModels.get(queueId);
        //一次读取多条consumeQueue的数据内容
        List<byte[]> consumeQueueContentList = consumeQueueMMapFileModel.readContent(consumeQueueOffset, batchSize);
        List<ConsumeMsgCommitLogDTO> commitLogBodyContentList = new ArrayList<>();
        for (byte[] content : consumeQueueContentList) {
            //根据consumeQueue的内容确定commitLog的读取位置
            ConsumeQueueDetailModel consumeQueueDetailModel = new ConsumeQueueDetailModel();
            consumeQueueDetailModel.buildFromBytes(content);
            CommitLogMMapFileModel commitLogMMapFileModel = CommonCache.getCommitLogMMapFileModelManager().get(topic);
            ConsumeMsgCommitLogDTO commitLogContent = commitLogMMapFileModel.readContent(consumeQueueDetailModel.getMsgIndex(), consumeQueueDetailModel.getMsgLength());
            commitLogContent.setRetryTimes(consumeQueueDetailModel.getRetryTimes());
            commitLogBodyContentList.add(commitLogContent);
        }

        return commitLogBodyContentList;
    }

    /**
     * 更新consumeQueue-offset的值
     *
     * @return
     */
    public boolean ack(String topic, String consumeGroup, Integer queueId) {
        try {
            ConsumeQueueOffsetModel.OffsetTable offsetTable = CommonCache.getConsumeQueueOffsetModel().getOffsetTable();
            Map<String, ConsumeQueueOffsetModel.ConsumerGroupDetail> consumerGroupDetailMap = offsetTable.getTopicConsumerGroupDetail();
            ConsumeQueueOffsetModel.ConsumerGroupDetail consumerGroupDetail = consumerGroupDetailMap.get(topic);
            Map<String, String> consumeQueueOffsetDetailMap = consumerGroupDetail.getConsumerGroupDetailMap().get(consumeGroup);
            String offsetStrInfo = consumeQueueOffsetDetailMap.get(String.valueOf(queueId));
            String[] offsetStrArr = offsetStrInfo.split("#");
            String fileName = offsetStrArr[0];
            Integer currentOffset = Integer.valueOf(offsetStrArr[1]);
            currentOffset += 16;
            consumeQueueOffsetDetailMap.put(String.valueOf(queueId), fileName + "#" + currentOffset);
        } catch (Exception e) {
            System.err.println("ack操作异常");
            e.printStackTrace();
        } finally {
        }
        return true;
    }
}
