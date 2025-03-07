package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.ConsumeMsgAckEvent;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.ConsumeMsgAckReqDTO;
import com.zhb.common.dto.ConsumeMsgAckRespDTO;
import com.zhb.common.enums.AckStatus;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * 消息确认(ACK)监听器
 * 用于处理消费者发送的消息确认请求事件
 * 负责验证确认请求的合法性并更新相应队列的消费偏移量
 */
@Slf4j
public class ConsumeMsgAckListener implements Listener<ConsumeMsgAckEvent> {

	/**
	 * 接收并处理消息确认事件
	 * 验证请求的合法性，并更新消费队列偏移量
	 *
	 * @param event 消息确认事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(ConsumeMsgAckEvent event) throws Exception {
		// 获取消息确认请求数据
		ConsumeMsgAckReqDTO consumeMsgAckReqDTO = event.getConsumeMsgAckReqDTO();
		String topic = consumeMsgAckReqDTO.getTopic();
		String consumeGroup = consumeMsgAckReqDTO.getConsumeGroup();
		Integer queueId = consumeMsgAckReqDTO.getQueueId();
		Integer ackCount = consumeMsgAckReqDTO.getAckCount();
		// 准备响应数据
		ConsumeMsgAckRespDTO consumeMsgAckRespDTO = new ConsumeMsgAckRespDTO();
		consumeMsgAckRespDTO.setMsgId(event.getMsgId());
		// 验证主题是否存在
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			//topic不存在，ack失败
			consumeMsgAckRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgAckRespDTO)));
			return;
		}
		// 验证消费组是否存在
		Map<String, List<ConsumerInstance>> consumerInstanceMap = CommonCache.getConsumeHoldMap().get(topic);
		if (consumerInstanceMap == null || consumerInstanceMap.isEmpty()) {
			consumeMsgAckRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgAckRespDTO)));
			return;
		}
		// 验证消费组下是否有消费者实例
		List<ConsumerInstance> consumeGroupInstances = consumerInstanceMap.get(consumeGroup);
		if (CollectionUtils.isEmpty(consumeGroupInstances)) {
			consumeMsgAckRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgAckRespDTO)));
			return;
		}
		// 验证当前请求的消费者是否存在于消费者组中
		String currentConsumeReqId = consumeMsgAckReqDTO.getIp() + ":" + consumeMsgAckReqDTO.getPort();
		ConsumerInstance matchInstance = consumeGroupInstances.stream().filter(item -> item.getConsumerReqId().equals(currentConsumeReqId)).findAny().orElse(null);
		if (matchInstance == null) {
			consumeMsgAckRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgAckRespDTO)));
			return;
		}
		// 执行确认操作，更新消费偏移量
		// 根据确认的消息数量(ackCount)多次调用确认方法
		for (int i = 0; i < ackCount; i++) {
			CommonCache.getConsumeQueueConsumeHandler().ack(topic, consumeGroup, queueId);
		}
		log.info("broker receive offset value ,topic is {},consumeGroup is {},queueId is {},ackCount is {}",
			topic, consumeGroup, queueId, ackCount);
		// 发送确认成功响应
		consumeMsgAckRespDTO.setAckStatus(AckStatus.SUCCESS.getCode());
		TcpMsg tcpMsg = new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
			JSON.toJSONBytes(consumeMsgAckRespDTO));
		event.getChannelHandlerContext().writeAndFlush(tcpMsg);
	}
}
