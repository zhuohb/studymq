package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.collections4.CollectionUtils;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.ConsumeMsgRetryEvent;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.broker.timewheel.DelayMessageDTO;
import com.zhb.broker.timewheel.SlotStoreTypeEnum;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.*;
import com.zhb.common.enums.AckStatus;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import com.zhb.common.event.model.Event;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created at 2024/7/21
 * @Description 消息重试监听器
 */
public class ConsumeMsgRetryListener implements Listener<ConsumeMsgRetryEvent> {

    private final Logger logger = LoggerFactory.getLogger(ConsumeMsgRetryListener.class);

    private static final List<Integer> RETRY_STEP = Arrays.asList(3, 5, 10, 15, 30);

    @Override
    public void onReceive(ConsumeMsgRetryEvent event) throws Exception {
        logger.info("consume msg retry handler,event:{}", JSON.toJSONString(event));
        ConsumeMsgRetryRespDTO consumeMsgRetryRespDTO = new ConsumeMsgRetryRespDTO();
        consumeMsgRetryRespDTO.setMsgId(event.getMsgId());
        ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO = event.getConsumeMsgRetryReqDTO();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) event.getChannelHandlerContext().channel().remoteAddress();
        //参数校验合格了才能继续往下ack
        for (ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO : consumeMsgRetryReqDTO.getConsumeMsgRetryReqDetailDTOList()) {
            //如果参数异常，中间会抛出异常，不会继续后续的ack和重新发送topic
            consumeMsgRetryReqDetailDTO.setIp(inetSocketAddress.getHostString());
            consumeMsgRetryReqDetailDTO.setPort(inetSocketAddress.getPort());
            this.checkParam(consumeMsgRetryRespDTO, event, consumeMsgRetryReqDetailDTO);
        }
        for (ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO : consumeMsgRetryReqDTO.getConsumeMsgRetryReqDetailDTOList()) {
            this.ackAndSendToRetryTopic(consumeMsgRetryReqDetailDTO, event);
        }
        consumeMsgRetryRespDTO.setAckStatus(AckStatus.SUCCESS.getCode());
        TcpMsg retryMsg = new TcpMsg(BrokerResponseCode.CONSUME_MSG_RETRY_RESP.getCode(), JSON.toJSONBytes(consumeMsgRetryRespDTO));
        event.getChannelHandlerContext().writeAndFlush(retryMsg);
    }

    private void ackAndSendToRetryTopic(ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO, Event event) {
        String topic = consumeMsgRetryReqDetailDTO.getTopic();
        Integer queueId = consumeMsgRetryReqDetailDTO.getQueueId();
        String consumeGroup = consumeMsgRetryReqDetailDTO.getConsumerGroup();
        //先保障后续的数据内容可以继续处理，避免出现消息大量堆积情况
        CommonCache.getConsumeQueueConsumeHandler().ack(topic, consumeGroup, queueId);
        //需要被重试的消息offset地址存储到retry主题里，当时间到了之后 重新取出推到重试队列的专用主题中
        Integer commitLogMsgLength = consumeMsgRetryReqDetailDTO.getCommitLogMsgLength();
        Long commitLogOffset = consumeMsgRetryReqDetailDTO.getCommitLogOffset();
        MessageRetryDTO messageRetryDTO = new MessageRetryDTO();
        messageRetryDTO.setTopic(topic);
        messageRetryDTO.setQueueId(consumeMsgRetryReqDetailDTO.getQueueId());
        messageRetryDTO.setConsumeGroup(consumeMsgRetryReqDetailDTO.getConsumerGroup());
        messageRetryDTO.setSourceCommitLogOffset(Math.toIntExact(commitLogOffset));
        messageRetryDTO.setSourceCommitLogSize(commitLogMsgLength);
        messageRetryDTO.setCurrentRetryTimes(consumeMsgRetryReqDetailDTO.getRetryTime());
        Integer nextRetryTimeStep = RETRY_STEP.get(consumeMsgRetryReqDetailDTO.getRetryTime());
        MessageDTO messageDTO = new MessageDTO();
        messageDTO.setMsgId(event.getMsgId());
        //塞入重试信息的dto对象的字节数组
        messageDTO.setBody(JSON.toJSONBytes(messageRetryDTO));
        messageDTO.setRetry(true);
        if (nextRetryTimeStep == null) {
            //超过重试次数上限 往dead_queue的commitLog文件中写入即可
            messageDTO.setTopic("dead_queue");
        } else {
            long nextRetryTime = System.currentTimeMillis() + (nextRetryTimeStep * 1000);
            messageDTO.setTopic("retry");
            messageRetryDTO.setNextRetryTime(nextRetryTime);
        }

        try {
            CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //扔入时间轮组件中，实现消息延迟重试效果
        DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
        delayMessageDTO.setData(messageRetryDTO);
        delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.MESSAGE_RETRY_DTO);
        delayMessageDTO.setDelay(nextRetryTimeStep);
        delayMessageDTO.setNextExecuteTime(System.currentTimeMillis() + nextRetryTimeStep * 1000);
        CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
    }

    private void checkParam(ConsumeMsgRetryRespDTO consumeMsgRetryRespDTO, Event event, ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO) {
        String topic = consumeMsgRetryReqDetailDTO.getTopic();
        String consumeGroup = consumeMsgRetryReqDetailDTO.getConsumerGroup();
        EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
        if (eagleMqTopicModel == null) {
            //topic不存在，ack失败
            consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
            event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.CONSUME_MSG_RETRY_RESP.getCode(),
                    com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
            throw new RuntimeException("checkParam error");
        }
        Map<String, List<ConsumerInstance>> consumerInstanceMap = CommonCache.getConsumeHoldMap().get(topic);
        if (consumerInstanceMap == null || consumerInstanceMap.isEmpty()) {
            consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
            event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
                    com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
            throw new RuntimeException("checkParam error");
        }
        List<ConsumerInstance> consumeGroupInstances = consumerInstanceMap.get(consumeGroup);
        if (CollectionUtils.isEmpty(consumeGroupInstances)) {
            consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
            event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
                    com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
            throw new RuntimeException("checkParam error");
        }
        String currentConsumeReqId = consumeMsgRetryReqDetailDTO.getIp() + ":" + consumeMsgRetryReqDetailDTO.getPort();
        ConsumerInstance matchInstance = consumeGroupInstances.stream().filter(item -> {
            return item.getConsumerReqId().equals(currentConsumeReqId);
        }).findAny().orElse(null);
        if (matchInstance == null) {
            consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
            event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
                    com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
            throw new RuntimeException("checkParam error");
        }
    }
}
