package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson2.JSON;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 消息重试监听器
 * 负责处理消费失败需要重试的消息
 * 提供延迟重试机制和递增的重试间隔，最终处理死信队列逻辑
 */
@Slf4j
public class ConsumeMsgRetryListener implements Listener<ConsumeMsgRetryEvent> {

	/**
	 * 重试延迟时间步进列表（单位：秒）
	 * 随着重试次数增加，延迟时间逐渐增大
	 */
	private static final List<Integer> RETRY_STEP = Arrays.asList(3, 5, 10, 15, 30);

	/**
	 * 接收并处理消息重试事件
	 * 校验请求参数的合法性，确认(ACK)原消息并将消息放入重试队列
	 *
	 * @param event 消息重试事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(ConsumeMsgRetryEvent event) throws Exception {
		log.info("consume msg retry handler,event:{}", JSON.toJSONString(event));
		// 准备响应数据
		ConsumeMsgRetryRespDTO consumeMsgRetryRespDTO = new ConsumeMsgRetryRespDTO();
		consumeMsgRetryRespDTO.setMsgId(event.getMsgId());
		// 获取重试请求数据
		ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO = event.getConsumeMsgRetryReqDTO();
		InetSocketAddress inetSocketAddress = (InetSocketAddress) event.getChannelHandlerContext().channel().remoteAddress();
		// 参数校验合格了才能继续往下ack
		for (ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO : consumeMsgRetryReqDTO.getConsumeMsgRetryReqDetailDTOList()) {
			//如果参数异常，中间会抛出异常，不会继续后续的ack和重新发送topic
			consumeMsgRetryReqDetailDTO.setIp(inetSocketAddress.getHostString());
			consumeMsgRetryReqDetailDTO.setPort(inetSocketAddress.getPort());
			this.checkParam(consumeMsgRetryRespDTO, event, consumeMsgRetryReqDetailDTO);
		}
		// 所有参数校验通过后，逐条处理消息的确认和重试
		for (ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO : consumeMsgRetryReqDTO.getConsumeMsgRetryReqDetailDTOList()) {
			this.ackAndSendToRetryTopic(consumeMsgRetryReqDetailDTO, event);
		}
		// 发送成功响应
		consumeMsgRetryRespDTO.setAckStatus(AckStatus.SUCCESS.getCode());
		TcpMsg retryMsg = new TcpMsg(BrokerResponseCode.CONSUME_MSG_RETRY_RESP.getCode(), JSON.toJSONBytes(consumeMsgRetryRespDTO));
		event.getChannelHandlerContext().writeAndFlush(retryMsg);
	}

	/**
	 * 确认消息并将消息发送到重试主题
	 * 更新消费偏移量并将消息入队到重试队列，使用时间轮实现延迟重试
	 *
	 * @param consumeMsgRetryReqDetailDTO 消息重试请求详情
	 * @param event                       原始事件对象
	 */
	private void ackAndSendToRetryTopic(ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO, Event event) {
		String topic = consumeMsgRetryReqDetailDTO.getTopic();
		Integer queueId = consumeMsgRetryReqDetailDTO.getQueueId();
		String consumeGroup = consumeMsgRetryReqDetailDTO.getConsumerGroup();

		//先保障后续的数据内容可以继续处理，避免出现消息大量堆积情况
		CommonCache.getConsumeQueueConsumeHandler().ack(topic, consumeGroup, queueId);

		//需要被重试的消息offset地址存储到retry主题里，当时间到了之后 重新取出推到重试队列的专用主题中
		Integer commitLogMsgLength = consumeMsgRetryReqDetailDTO.getCommitLogMsgLength();
		Long commitLogOffset = consumeMsgRetryReqDetailDTO.getCommitLogOffset();

		// 构建消息重试数据传输对象
		MessageRetryDTO messageRetryDTO = new MessageRetryDTO();
		messageRetryDTO.setTopic(topic);
		messageRetryDTO.setQueueId(consumeMsgRetryReqDetailDTO.getQueueId());
		messageRetryDTO.setConsumeGroup(consumeMsgRetryReqDetailDTO.getConsumerGroup());
		messageRetryDTO.setSourceCommitLogOffset(Math.toIntExact(commitLogOffset));
		messageRetryDTO.setSourceCommitLogSize(commitLogMsgLength);
		messageRetryDTO.setCurrentRetryTimes(consumeMsgRetryReqDetailDTO.getRetryTime());

		// 获取下一次重试的延迟时间
		Integer nextRetryTimeStep = RETRY_STEP.get(consumeMsgRetryReqDetailDTO.getRetryTime());

		// 构建消息数据传输对象
		MessageDTO messageDTO = new MessageDTO();
		messageDTO.setMsgId(event.getMsgId());
		//塞入重试信息的dto对象的字节数组
		messageDTO.setBody(JSON.toJSONBytes(messageRetryDTO));
		messageDTO.setRetry(true);

		// 判断是否超过重试次数上限
		if (nextRetryTimeStep == null) {
			//超过重试次数上限 转入死信队列
			messageDTO.setTopic("dead_queue");
		} else {
			// 计算下一次重试的时间点
			long nextRetryTime = System.currentTimeMillis() + (nextRetryTimeStep * 1000);
			messageDTO.setTopic("retry");
			messageRetryDTO.setNextRetryTime(nextRetryTime);
		}

		// 将消息添加到CommitLog
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

	/**
	 * 校验请求参数
	 * 验证主题、消费组和消费者实例的合法性
	 *
	 * @param consumeMsgRetryRespDTO      响应数据传输对象
	 * @param event                       事件对象
	 * @param consumeMsgRetryReqDetailDTO 请求详情数据传输对象
	 */
	private void checkParam(ConsumeMsgRetryRespDTO consumeMsgRetryRespDTO, Event event, ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO) {
		String topic = consumeMsgRetryReqDetailDTO.getTopic();
		String consumeGroup = consumeMsgRetryReqDetailDTO.getConsumerGroup();

		// 验证主题是否存在
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			//topic不存在，ack失败
			consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.CONSUME_MSG_RETRY_RESP.getCode(),
				com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
			throw new RuntimeException("checkParam error");
		}

		// 验证消费组是否存在
		Map<String, List<ConsumerInstance>> consumerInstanceMap = CommonCache.getConsumeHoldMap().get(topic);
		if (consumerInstanceMap == null || consumerInstanceMap.isEmpty()) {
			consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
			throw new RuntimeException("checkParam error");
		}

		// 验证消费组下是否有消费者实例
		List<ConsumerInstance> consumeGroupInstances = consumerInstanceMap.get(consumeGroup);
		if (CollectionUtils.isEmpty(consumeGroupInstances)) {
			consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
			throw new RuntimeException("checkParam error");
		}

		// 验证当前请求的消费者是否存在于消费者组中
		String currentConsumeReqId = consumeMsgRetryReqDetailDTO.getIp() + ":" + consumeMsgRetryReqDetailDTO.getPort();
		ConsumerInstance matchInstance = consumeGroupInstances.stream().filter(item -> item.getConsumerReqId().equals(currentConsumeReqId)).findAny().orElse(null);
		if (matchInstance == null) {
			consumeMsgRetryRespDTO.setAckStatus(AckStatus.FAIL.getCode());
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.BROKER_UPDATE_CONSUME_OFFSET_RESP.getCode(),
				com.alibaba.fastjson.JSON.toJSONBytes(consumeMsgRetryRespDTO)));
			throw new RuntimeException("checkParam error");
		}
	}
}
