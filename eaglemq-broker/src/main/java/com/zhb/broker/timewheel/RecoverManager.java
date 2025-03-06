package com.zhb.broker.timewheel;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections4.CollectionUtils;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.ConsumeQueueConsumeReqModel;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.enums.MessageSendWay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author idea
 * @Date: Created at 2024/8/11
 * @Description 数据恢复服务
 */
public class RecoverManager {

    private final Logger logger = LoggerFactory.getLogger(RecoverManager.class);

    /**
     * 延迟消息数据恢复能力
     */
    public void doDelayMessageRecovery() {
        //读取mmap中的数据内容
        String topic = "delay_queue";
        String consumeGroup = "broker_delay_message_recovery_job";
        Integer queueId = 0;
        ConsumeQueueConsumeReqModel consumeQueueConsumeReqModel = new ConsumeQueueConsumeReqModel();
        consumeQueueConsumeReqModel.setConsumeGroup(consumeGroup);
        consumeQueueConsumeReqModel.setQueueId(queueId);
        consumeQueueConsumeReqModel.setTopic(topic);
        consumeQueueConsumeReqModel.setBatchSize(1);
        long currentTime = System.currentTimeMillis();
        while (true) {
            List<ConsumeMsgCommitLogDTO> consumeMsgCommitLogDTOS = CommonCache.getConsumeQueueConsumeHandler().consume(consumeQueueConsumeReqModel);
            if (CollectionUtils.isEmpty(consumeMsgCommitLogDTOS)) {
                break;
            }
            ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO = consumeMsgCommitLogDTOS.get(0);
            byte[] commitLogBody = consumeMsgCommitLogDTO.getBody();

            //数据恢复过程中需要考虑的问题 1.不需要，已经过期的数据可以考虑直接丢弃/直接扔到下一个slot里 2.delay需要重新计算
            DelayMessageDTO delayMessageDTO = JSON.parseObject(new String(commitLogBody), DelayMessageDTO.class);
            long nextExecuteTime = delayMessageDTO.getNextExecuteTime();
            if(nextExecuteTime <= currentTime) {
                //todo  如果重新扔到文件中，可能在宕机之前数据已经消费过一次了，因此可能会导致重复消费的问题
                tryInputToCommitLog(delayMessageDTO);
            } else {
                tryInputToTimeWheelAgain(delayMessageDTO);
            }
            logger.info("重新恢复，获取延迟消息对象：{}", JSON.toJSONString(delayMessageDTO));
            CommonCache.getConsumeQueueConsumeHandler().ack(topic,consumeGroup,queueId);
        }
    }

    /**
     * 重新计算延迟消息的延迟时间，扔到时间轮里
     * @param delayMessageDTO
     */
    private void tryInputToTimeWheelAgain(DelayMessageDTO delayMessageDTO) {
        //计算出延迟的时间点相对于当前还有多少秒
        int remainSeconds = (int) ((delayMessageDTO.getNextExecuteTime() - System.currentTimeMillis()) / 1000);
        delayMessageDTO.setDelay(remainSeconds);
        CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
    }

    /**
     * 延迟消息已经到期，直接人到commitLog中
     *
     * @param delayMessageDTO
     */
    private void tryInputToCommitLog(DelayMessageDTO delayMessageDTO) {
        //时间点已经流逝
        MessageDTO messageDTO = JSON.parseObject(JSON.toJSONString(delayMessageDTO.getData()), MessageDTO.class);
        messageDTO.setDelay(0);
        messageDTO.setSendWay(MessageSendWay.ASYNC.getCode());
        System.out.println("延迟消息重新入commitLog:" + JSON.toJSONString(messageDTO));
        try {
            CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO,null);
        } catch (Exception e) {
           logger.error("try input to commit log error: ",e);
        }
    }
}
