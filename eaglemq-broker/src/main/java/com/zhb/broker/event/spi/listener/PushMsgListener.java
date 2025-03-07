package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.PushMsgEvent;
import com.zhb.broker.model.TxMessageAckModel;
import com.zhb.broker.timewheel.DelayMessageDTO;
import com.zhb.broker.timewheel.SlotStoreTypeEnum;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.SendMessageToBrokerResponseDTO;
import com.zhb.common.dto.TxMessageDTO;
import com.zhb.common.enums.*;
import com.zhb.common.event.Listener;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 消息推送监听器
 * 负责处理客户端发送到Broker的各类消息推送事件
 * 包括普通消息、延迟消息和事务消息的处理
 */
public class PushMsgListener implements Listener<PushMsgEvent> {

	private static final Logger log = LoggerFactory.getLogger(PushMsgListener.class);

	/**
	 * 接收并处理消息推送事件
	 * 根据消息类型分发到不同的处理方法
	 *
	 * @param event 消息推送事件对象
	 * @throws IOException 处理过程中可能发生的IO异常
	 */
	@Override
	public void onReceive(PushMsgEvent event) throws IOException {
		// 获取消息数据传输对象
		MessageDTO messageDTO = event.getMessageDTO();

		// 判断消息类型：延迟消息、半事务消息、事务确认消息或普通消息
		boolean isDelayMsg = messageDTO.getDelay() > 0;
		boolean isHalfMsg = messageDTO.getTxFlag() == TxMessageFlagEnum.HALF_MSG.getCode();
		boolean isRemainHalfAck = messageDTO.getTxFlag() == TxMessageFlagEnum.REMAIN_HALF_ACK.getCode();
		// 根据消息类型分发到不同的处理方法
		if (isDelayMsg) {
			this.appendDelayMsgHandler(messageDTO, event);
		} else if (isHalfMsg) {
			this.halfMsgHandler(messageDTO, event);
		} else if (isRemainHalfAck) {
			this.remainHalfMsgAckHandler(messageDTO, event);
		} else {
			this.appendDefaultMsgHandler(messageDTO, event);
		}
	}

	/**
	 * 处理事务消息的确认操作
	 * 根据事务状态决定是提交还是回滚事务消息
	 *
	 * @param messageDTO 消息数据传输对象
	 * @param event      消息推送事件对象
	 * @throws IOException 处理过程中可能发生的IO异常
	 */
	private void remainHalfMsgAckHandler(MessageDTO messageDTO, PushMsgEvent event) throws IOException {
		// 获取本地事务状态
		LocalTransactionState localTransactionState = LocalTransactionState.of(messageDTO.getLocalTxState());

		if (localTransactionState == LocalTransactionState.COMMIT) {
			// 如果是提交状态，移除事务映射并将消息写入CommitLog
			CommonCache.getTxMessageAckModelMap().remove(messageDTO.getMsgId());
			CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO);
			log.info("收到事务消息的commit请求");
		} else if (localTransactionState == LocalTransactionState.ROLLBACK) {
			// 如果是回滚状态，只移除事务映射
			CommonCache.getTxMessageAckModelMap().remove(messageDTO.getMsgId());
			log.info("收到事务消息的rollback请求");
		}
		// 告知客户端事务确认处理成功
		SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
		sendMsgResp.setMsgId(messageDTO.getMsgId());
		sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
		sendMsgResp.setDesc("send tx remain ack msg success");
		TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.REMAIN_ACK_MSG_SEND_SUCCESS.getCode(), JSON.toJSONBytes(sendMsgResp));
		event.getChannelHandlerContext().writeAndFlush(responseMsg);
	}

	/**
	 * 处理半事务消息
	 * 将半事务消息存入缓存并通过时间轮设置检查点
	 *
	 * @param messageDTO 消息数据传输对象
	 * @param event      消息推送事件对象
	 */
	private void halfMsgHandler(MessageDTO messageDTO, PushMsgEvent event) {
		// 创建事务确认模型并存入缓存
		TxMessageAckModel txMessageAckModel = new TxMessageAckModel();
		txMessageAckModel.setMessageDTO(messageDTO);
		txMessageAckModel.setChannelHandlerContext(event.getChannelHandlerContext());
		txMessageAckModel.setFirstSendTime(System.currentTimeMillis());
		CommonCache.getTxMessageAckModelMap().put(messageDTO.getMsgId(), txMessageAckModel);

		// 通过时间轮设置事务检查点
		TxMessageDTO txMessageDTO = new TxMessageDTO();
		txMessageDTO.setMsgId(messageDTO.getMsgId());
		long currentTime = System.currentTimeMillis();
		DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
		delayMessageDTO.setData(txMessageDTO);
		delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.TX_MESSAGE_DTO);
		delayMessageDTO.setNextExecuteTime(currentTime + 3 * 1000);
		delayMessageDTO.setDelay(3);
		CommonCache.getTimeWheelModelManager().add(delayMessageDTO);

		// 告知客户端半事务消息接收成功
		SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
		sendMsgResp.setMsgId(messageDTO.getMsgId());
		sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
		sendMsgResp.setDesc("send tx half msg success");
		TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.HALF_MSG_SEND_SUCCESS.getCode(), JSON.toJSONBytes(sendMsgResp));
		event.getChannelHandlerContext().writeAndFlush(responseMsg);
	}

	/**
	 * 处理普通消息
	 * 将普通消息直接写入CommitLog
	 *
	 * @param messageDTO 消息数据传输对象
	 * @param event 消息推送事件对象
	 * @throws IOException 写入过程中可能发生的IO异常
	 */
	private void appendDefaultMsgHandler(MessageDTO messageDTO, PushMsgEvent event) throws IOException {
		// 将普通消息追加到CommitLog中
		CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO, event);
	}


	/**
	 * 处理延迟消息
	 * 将延迟消息存入时间轮和特殊延迟队列
	 *
	 * @param messageDTO 消息数据传输对象
	 * @param event 消息推送事件对象
	 * @throws IOException 处理过程中可能发生的IO异常
	 */
	private void appendDelayMsgHandler(MessageDTO messageDTO, PushMsgEvent event) throws IOException {
		// 获取延迟时间并验证合法性
		int delaySeconds = messageDTO.getDelay();
		AssertUtils.isTrue(delaySeconds <= 3600, "too large delay seconds");

		// 创建延迟消息对象并添加到时间轮
		DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
		delayMessageDTO.setDelay(messageDTO.getDelay());
		delayMessageDTO.setData(messageDTO);
		delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.DELAY_MESSAGE_DTO);
		delayMessageDTO.setNextExecuteTime(System.currentTimeMillis() + delaySeconds * 1000);
		CommonCache.getTimeWheelModelManager().add(delayMessageDTO);

		// 将延迟消息持久化到特殊延迟队列中
		MessageDTO delayMessage = new MessageDTO();
		delayMessage.setBody(JSON.toJSONBytes(delayMessageDTO));
		delayMessage.setTopic("delay_queue");
		delayMessage.setQueueId(0);
		delayMessage.setSendWay(MessageSendWay.ASYNC.getCode());
		CommonCache.getCommitLogAppendHandler().appendMsg(delayMessage, event);

		// 告知客户端延迟消息接收成功
		SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
		sendMsgResp.setMsgId(messageDTO.getMsgId());
		sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
		sendMsgResp.setDesc("send delay msg success");
		TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.SEND_MSG_RESP.getCode(), JSON.toJSONBytes(sendMsgResp));
		event.getChannelHandlerContext().writeAndFlush(responseMsg);
	}
}
