package com.zhb.broker.timewheel;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.ConsumeQueueConsumeReqModel;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.enums.MessageSendWay;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * 数据恢复管理器
 * 负责系统重启或宕机后的消息恢复工作
 * 主要处理延迟消息队列中的数据恢复，确保消息不丢失
 */
@Slf4j
public class RecoverManager {

	/**
	 * 执行延迟消息恢复
	 * 从持久化存储中读取延迟消息，根据消息状态重新放入时间轮或提交日志
	 * 确保系统重启后延迟消息能够被正确处理
	 */
	public void doDelayMessageRecovery() {
		// 从特定的延迟队列主题中读取数据
		String topic = "delay_queue";
		String consumeGroup = "broker_delay_message_recovery_job";
		Integer queueId = 0;
		ConsumeQueueConsumeReqModel consumeQueueConsumeReqModel = new ConsumeQueueConsumeReqModel();
		consumeQueueConsumeReqModel.setConsumeGroup(consumeGroup);
		consumeQueueConsumeReqModel.setQueueId(queueId);
		consumeQueueConsumeReqModel.setTopic(topic);
		consumeQueueConsumeReqModel.setBatchSize(1);
		long currentTime = System.currentTimeMillis();

		// 持续获取并处理延迟消息，直到队列为空
		while (true) {
			// 从消费队列中获取一批消息
			List<ConsumeMsgCommitLogDTO> consumeMsgCommitLogDTOS = CommonCache.getConsumeQueueConsumeHandler().consume(consumeQueueConsumeReqModel);
			if (CollectionUtils.isEmpty(consumeMsgCommitLogDTOS)) {
				break;
			}

			// 获取消息内容并解析为延迟消息对象
			ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO = consumeMsgCommitLogDTOS.get(0);
			byte[] commitLogBody = consumeMsgCommitLogDTO.getBody();

			// 数据恢复需要考虑：1.已过期的消息可直接丢弃或放入下一个slot 2.需要重新计算延迟时间
			DelayMessageDTO delayMessageDTO = JSON.parseObject(new String(commitLogBody), DelayMessageDTO.class);
			long nextExecuteTime = delayMessageDTO.getNextExecuteTime();

			// 根据执行时间判断消息处理方式
			if (nextExecuteTime <= currentTime) {
				// 若消息已过期，直接放入提交日志（注意：可能导致重复消费）
				tryInputToCommitLog(delayMessageDTO);
			} else {
				// 若消息未过期，重新计算延迟时间并放回时间轮
				tryInputToTimeWheelAgain(delayMessageDTO);
			}
			log.info("重新恢复，获取延迟消息对象：{}", JSON.toJSONString(delayMessageDTO));

			// 确认消息已处理
			CommonCache.getConsumeQueueConsumeHandler().ack(topic, consumeGroup, queueId);
		}
	}

	/**
	 * 重新计算延迟消息时间并放入时间轮
	 * 根据当前时间计算剩余延迟秒数，确保消息在正确的时间点被处理
	 *
	 * @param delayMessageDTO 延迟消息数据传输对象
	 */
	private void tryInputToTimeWheelAgain(DelayMessageDTO delayMessageDTO) {
		// 计算剩余延迟秒数（下次执行时间减去当前时间）
		int remainSeconds = (int) ((delayMessageDTO.getNextExecuteTime() - System.currentTimeMillis()) / 1000);
		delayMessageDTO.setDelay(remainSeconds);
		// 将消息重新添加到时间轮中
		CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
	}

	/**
	 * 处理已过期的延迟消息
	 * 将过期的延迟消息直接投递到提交日志，使其立即被处理
	 *
	 * @param delayMessageDTO 延迟消息数据传输对象
	 */
	private void tryInputToCommitLog(DelayMessageDTO delayMessageDTO) {
		// 将延迟消息转换为普通消息
		MessageDTO messageDTO = JSON.parseObject(JSON.toJSONString(delayMessageDTO.getData()), MessageDTO.class);
		messageDTO.setDelay(0);
		messageDTO.setSendWay(MessageSendWay.ASYNC.getCode());
		System.out.println("延迟消息重新入commitLog:" + JSON.toJSONString(messageDTO));
		try {
			// 将消息追加到提交日志中
			CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO, null);
		} catch (Exception e) {
			log.error("try input to commit log error: ", e);
		}
	}
}
