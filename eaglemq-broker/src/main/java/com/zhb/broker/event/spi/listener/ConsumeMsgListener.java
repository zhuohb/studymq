package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.ConsumeMsgEvent;
import com.zhb.broker.model.ConsumeQueueConsumeReqModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.ConsumeMsgBaseRespDTO;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;
import com.zhb.common.dto.ConsumeMsgReqDTO;
import com.zhb.common.dto.ConsumeMsgRespDTO;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 消费者拉取消息监听器
 * 负责处理消费者发送的消息拉取请求事件
 * 验证消费者权限并从相应队列拉取消息返回给客户端
 */
public class ConsumeMsgListener implements Listener<ConsumeMsgEvent> {
	/**
	 * 接收并处理消息拉取事件
	 * 验证消费者权限，从分配给该消费者的队列中拉取消息
	 *
	 * @param event 消息拉取事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(ConsumeMsgEvent event) throws Exception {
		// 获取消息拉取请求数据
		ConsumeMsgReqDTO consumeMsgReqDTO = event.getConsumeMsgReqDTO();
		String currentReqId = consumeMsgReqDTO.getIp() + ":" + consumeMsgReqDTO.getPort();
		String topic = consumeMsgReqDTO.getTopic();
		// 构建消费者实例并添加到消费者池
		ConsumerInstance consumerInstance = new ConsumerInstance();
		consumerInstance.setIp(consumeMsgReqDTO.getIp());
		consumerInstance.setPort(consumeMsgReqDTO.getPort());
		consumerInstance.setConsumerReqId(currentReqId);
		consumerInstance.setTopic(consumeMsgReqDTO.getTopic());
		consumerInstance.setConsumeGroup(consumeMsgReqDTO.getConsumeGroup());
		consumerInstance.setBatchSize(consumeMsgReqDTO.getBatchSize());
		// 加入到消费池中
		CommonCache.getConsumerInstancePool().addInstancePool(consumerInstance);
		// 准备响应数据
		ConsumeMsgBaseRespDTO consumeMsgBaseRespDTO = new ConsumeMsgBaseRespDTO();
		List<ConsumeMsgRespDTO> consumeMsgRespDTOS = new ArrayList<>();
		consumeMsgBaseRespDTO.setConsumeMsgRespDTOList(consumeMsgRespDTOS);
		consumeMsgBaseRespDTO.setMsgId(event.getMsgId());
		// 获取主题下的消费组映射
		Map<String, List<ConsumerInstance>> consumeGroupMap = CommonCache.getConsumeHoldMap().get(topic);
		// 检查消费组是否已经过重平衡，如果没有则返回空数据
		if (consumeGroupMap == null) {
			// 直接返回空数据，表示队列尚未分配
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.CONSUME_MSG_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgBaseRespDTO)));
			return;
		}
		// 检查指定消费组下是否有消费者实例
		List<ConsumerInstance> consumerInstances = consumeGroupMap.get(consumeMsgReqDTO.getConsumeGroup());
		if (CollectionUtils.isEmpty(consumerInstances)) {
			//直接返回空数据 表示该消费组下暂无消费者
			event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.CONSUME_MSG_RESP.getCode(),
				JSON.toJSONBytes(consumeMsgBaseRespDTO)));
			return;
		}
		// 遍历消费组下的所有消费者实例，查找匹配当前请求的消费者
		for (ConsumerInstance instance : consumerInstances) {
			if (instance.getConsumerReqId().equals(currentReqId)) {
				// 当前消费者有占有队列的权利，可以消费
				for (Integer queueId : instance.getQueueIdSet()) {
					// 构建消费请求模型
					ConsumeQueueConsumeReqModel consumeQueueConsumeReqModel = new ConsumeQueueConsumeReqModel();
					consumeQueueConsumeReqModel.setTopic(topic);
					consumeQueueConsumeReqModel.setQueueId(queueId);
					consumeQueueConsumeReqModel.setBatchSize(instance.getBatchSize());
					consumeQueueConsumeReqModel.setConsumeGroup(instance.getConsumeGroup());
					List<ConsumeMsgCommitLogDTO> commitLogContentList = CommonCache.getConsumeQueueConsumeHandler().consume(consumeQueueConsumeReqModel);
					// 从消费队列中拉取消息
					ConsumeMsgRespDTO consumeMsgRespDTO = new ConsumeMsgRespDTO();
					// 构建响应数据
					consumeMsgRespDTO.setQueueId(queueId);
					consumeMsgRespDTO.setCommitLogContentList(commitLogContentList);
					consumeMsgRespDTOS.add(consumeMsgRespDTO);
				}
			}
		}
		// 发送响应消息给客户端
		byte[] body = JSON.toJSONBytes(consumeMsgBaseRespDTO);
		TcpMsg respMsg = new TcpMsg(BrokerResponseCode.CONSUME_MSG_RESP.getCode(),
			body);
		event.getChannelHandlerContext().writeAndFlush(respMsg);
	}
}
