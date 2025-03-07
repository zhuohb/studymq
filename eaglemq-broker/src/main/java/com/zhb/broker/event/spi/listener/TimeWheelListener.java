package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.core.CommitLogMMapFileModel;
import com.zhb.broker.event.model.TimeWheelEvent;
import com.zhb.broker.model.TxMessageAckModel;
import com.zhb.broker.timewheel.DelayMessageDTO;
import com.zhb.broker.timewheel.SlotStoreTypeEnum;
import com.zhb.broker.timewheel.TimeWheelSlotModel;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.*;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 时间轮监听器
 * 负责处理时间轮触发的各类计时事件
 * 包括消息重试、延迟消息投递和事务消息回查
 */
@Slf4j
public class TimeWheelListener implements Listener<TimeWheelEvent> {

	/**
	 * 接收并处理时间轮触发的事件
	 * 根据槽中存储的数据类型分发到不同的处理逻辑
	 *
	 * @param event 时间轮事件对象，包含触发的时间槽列表
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(TimeWheelEvent event) throws Exception {
		// 获取需要处理的时间槽模型列表
		List<TimeWheelSlotModel> timeWheelSlotModelList = event.getTimeWheelSlotModelList();
		if (CollectionUtils.isEmpty(timeWheelSlotModelList)) {
			log.error("timeWheelSlotModelList is empty");
			return;
		}

		// 遍历时间槽中的任务，根据类型分发处理
		for (TimeWheelSlotModel timeWheelSlotModel : timeWheelSlotModelList) {
			// 处理消息重试事件
			if (SlotStoreTypeEnum.MESSAGE_RETRY_DTO.getClazz().equals(timeWheelSlotModel.getStoreType())) {
				MessageRetryDTO messageRetryDTO = (MessageRetryDTO) timeWheelSlotModel.getData();
				this.messageRetryHandler(messageRetryDTO);
			}
			// 处理延迟消息投递事件
			else if (SlotStoreTypeEnum.DELAY_MESSAGE_DTO.getClazz().equals(timeWheelSlotModel.getStoreType())) {
				MessageDTO messageDTO = (MessageDTO) timeWheelSlotModel.getData();
				System.out.println("延迟消���重新入commitLog:" + JSON.toJSONString(messageDTO));
				// 延迟时间到，将消息写入CommitLog进行正常投递
				CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO, event);
			}
			// 处理事务消息回查事件
			else if (SlotStoreTypeEnum.TX_MESSAGE_DTO.getClazz().equals(timeWheelSlotModel.getStoreType())) {
				TxMessageDTO txMessageDTO = (TxMessageDTO) timeWheelSlotModel.getData();
				// 时间轮到期，检查ack缓存是否还有未提交的事务消息记录
				TxMessageAckModel txMessageAckModel = CommonCache.getTxMessageAckModelMap().get(txMessageDTO.getMsgId());
				if (txMessageAckModel == null) {
					// 事务消息已经被确认处理，无需进一步回查
					log.info("txMessageAckModel is already been ack");
					continue;
				}

				// 构建事务消息回调请求，询问客户端事务状态
				TxMessageCallbackReqDTO txMessageCallbackReqDTO = new TxMessageCallbackReqDTO();
				txMessageCallbackReqDTO.setMessageDTO(txMessageAckModel.getMessageDTO());
				TcpMsg tcpMsg = new TcpMsg(BrokerResponseCode.TX_CALLBACK_MSG.getCode(), JSON.toJSONBytes(txMessageCallbackReqDTO));

				// 发送回调请求并监听结果
				txMessageAckModel.getChannelHandlerContext().writeAndFlush(tcpMsg).addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture channelFuture) throws Exception {
						if (channelFuture.isSuccess()) {
							System.out.println("成功发送回调");
							// 重新将事务消息检查任务添加到时间轮中，等待下一次回查
							DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
							delayMessageDTO.setData(txMessageDTO);
							delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.TX_MESSAGE_DTO);
							delayMessageDTO.setNextExecuteTime(System.currentTimeMillis() + 3 * 1000);
							delayMessageDTO.setDelay(3);
							CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
						} else {
							// 客户端可能已经异常退出
							System.err.println("异常发送回调");
						}
					}
				});
			}
		}
	}

	/**
	 * 消息重试处理器
	 * 从原CommitLog中读取消息内容，重新构建消息发送到特定的重试主题
	 *
	 * @param messageRetryDTO 消息重试数据传输对象，包含原消息的位置信息
	 */
	private void messageRetryHandler(MessageRetryDTO messageRetryDTO) {
		// 从CommitLog中读取原始消息数据
		CommitLogMMapFileModel commitLogMMapFileModel = CommonCache.getCommitLogMMapFileModelManager().get(messageRetryDTO.getTopic());
		ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO = commitLogMMapFileModel.readContent(
			messageRetryDTO.getSourceCommitLogOffset(),
			messageRetryDTO.getSourceCommitLogSize());
		byte[] commitLogBody = consumeMsgCommitLogDTO.getBody();
		System.out.println("扔到重试topic的数据：" + new String(commitLogBody));

		// 构建重试消息对象
		MessageDTO messageDTO = new MessageDTO();
		messageDTO.setBody(commitLogBody);
		// 将消息写入特定命名格式的重试主题
		messageDTO.setTopic("retry%" + messageRetryDTO.getConsumeGroup());
		// 随机选择队列，避免消息集中在同一队列
		messageDTO.setQueueId(ThreadLocalRandom.current().nextInt(3));
		// 增加重试次数计数
		messageDTO.setCurrentRetryTimes(messageRetryDTO.getCurrentRetryTimes() + 1);
		log.info("重试次数增加：{}", messageDTO.getCurrentRetryTimes());

		// 将重试消息写入CommitLog
		try {
			CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
