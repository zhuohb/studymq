package com.zhb.broker.netty.broker;

import com.alibaba.fastjson2.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.*;
import com.zhb.broker.model.ConsumeMsgAckEvent;
import com.zhb.common.cache.BrokerServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.*;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;
import com.zhb.common.remote.SyncFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * Broker服务器消息处理器
 * 负责接收和处理客户端的各类请求，将TCP消息解析为事件并分发到相应的处理逻辑
 * 同时处理通道的生命周期事件和异常情况
 */
@Slf4j
@ChannelHandler.Sharable
public class BrokerServerHandler extends SimpleChannelInboundHandler {

	/**
	 * 事件总线，用于分发事件到对应的监听器
	 */
	private EventBus eventBus;

	/**
	 * 构造函数
	 *
	 * @param eventBus 事件总线实例，用于发布事件
	 */
	public BrokerServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理接收到的消息
	 * 根据消息的类型编码解析为不同的事件，并发布到事件总线
	 *
	 * @param channelHandlerContext 通道处理上下文，包含通道信息
	 * @param msg                   接收到的消息对象，通常是TcpMsg类型
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		// 转换为TcpMsg类型并获取消息编码和内容
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		Event event = null;

		// 根据消息编码创建对应的事件对象
		if (BrokerEventCode.PUSH_MSG.getCode() == code) {
			// 处理消息推送请求
			MessageDTO messageDTO = JSON.parseObject(body, MessageDTO.class);
			PushMsgEvent pushMsgEvent = new PushMsgEvent();
			pushMsgEvent.setMessageDTO(messageDTO);
			pushMsgEvent.setMsgId(messageDTO.getMsgId());
			log.info("收到消息推送内容:{},message is {}", new String(messageDTO.getBody()), JSON.toJSONString(messageDTO));
			event = pushMsgEvent;
		} else if (BrokerEventCode.CONSUME_MSG.getCode() == code) {
			// 处理消息消费请求
			ConsumeMsgReqDTO consumeMsgReqDTO = JSON.parseObject(body, ConsumeMsgReqDTO.class);
			// 获取消费者的网络地址信息
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
			consumeMsgReqDTO.setIp(inetSocketAddress.getHostString());
			consumeMsgReqDTO.setPort(inetSocketAddress.getPort());

			ConsumeMsgEvent consumeMsgEvent = new ConsumeMsgEvent();
			consumeMsgEvent.setConsumeMsgReqDTO(consumeMsgReqDTO);
			consumeMsgEvent.setMsgId(consumeMsgReqDTO.getMsgId());
			// 将消费者ID作为属性附加到通道上，用于后续识别
			channelHandlerContext.attr(AttributeKey.valueOf("consumer-reqId")).set(consumeMsgReqDTO.getIp() + ":" + consumeMsgReqDTO.getPort());
			event = consumeMsgEvent;
		} else if (BrokerEventCode.CONSUME_SUCCESS_MSG.getCode() == code) {
			// 处理消息消费确认请求
			ConsumeMsgAckReqDTO consumeMsgAckReqDTO = JSON.parseObject(body, ConsumeMsgAckReqDTO.class);
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
			consumeMsgAckReqDTO.setIp(inetSocketAddress.getHostString());
			consumeMsgAckReqDTO.setPort(inetSocketAddress.getPort());

			ConsumeMsgAckEvent consumeMsgAckEvent = new ConsumeMsgAckEvent();
			consumeMsgAckEvent.setConsumeMsgAckReqDTO(consumeMsgAckReqDTO);
			consumeMsgAckEvent.setMsgId(consumeMsgAckReqDTO.getMsgId());
			event = consumeMsgAckEvent;
		} else if (BrokerEventCode.CONSUME_LATER_MSG.getCode() == code) {
			// 处理消息延迟消费请求
			ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO = JSON.parseObject(body, ConsumeMsgRetryReqDTO.class);
			ConsumeMsgRetryEvent consumeMsgRetryEvent = new ConsumeMsgRetryEvent();
			consumeMsgRetryEvent.setMsgId(consumeMsgRetryReqDTO.getMsgId());
			consumeMsgRetryEvent.setConsumeMsgRetryReqDTO(consumeMsgRetryReqDTO);
			event = consumeMsgRetryEvent;
		} else if (BrokerEventCode.CREATE_TOPIC.getCode() == code) {
			// 处理主题创建请求
			CreateTopicReqDTO createTopicReqDTO = JSON.parseObject(body, CreateTopicReqDTO.class);
			CreateTopicEvent createTopicEvent = new CreateTopicEvent();
			createTopicEvent.setCreateTopicReqDTO(createTopicReqDTO);
			createTopicEvent.setMsgId(createTopicReqDTO.getMsgId());
			event = createTopicEvent;
		} else if (BrokerEventCode.START_SYNC_MSG.getCode() == code) {
			// 处理主从同步启动请求
			StartSyncReqDTO startSyncReqDTO = JSON.parseObject(body, StartSyncReqDTO.class);
			StartSyncEvent startSyncEvent = new StartSyncEvent();
			startSyncEvent.setMsgId(startSyncReqDTO.getMsgId());
			event = startSyncEvent;
		} else if (BrokerResponseCode.SLAVE_SYNC_RESP.getCode() == code) {
			// 处理从节点同步响应
			SlaveSyncRespDTO slaveSyncRespDTO = JSON.parseObject(body, SlaveSyncRespDTO.class);
			// 获取对应的同步Future并设置响应结果
			SyncFuture slaveAckRsp = BrokerServerSyncFutureManager.get(slaveSyncRespDTO.getMsgId());
			slaveAckRsp.setResponse(slaveSyncRespDTO);
		}

		// 如果创建了有效事件，将其发布到事件总线
		if (event != null) {
			event.setChannelHandlerContext(channelHandlerContext);
			eventBus.publish(event);
		}
	}

	/**
	 * 处理通道异常
	 * 记录异常信息但不主动关闭通道
	 *
	 * @param ctx   通道处理上下文
	 * @param cause 异常原因
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
		log.error("error is :", cause);
	}

	/**
	 * 处理通道关闭事件
	 * 在通道关闭时从消费者实例池中移除对应的消费者
	 *
	 * @param ctx 通道处理上下文
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		// 通道断开时，从消费者重平衡池中移除该消费者
		Object reqId = ctx.attr(AttributeKey.valueOf("consumer-reqId")).get();
		if (reqId == null) {
			return;
		}
		CommonCache.getConsumerInstancePool().removeFromInstancePool(String.valueOf(reqId));
	}

	/**
	 * 处理通道激活事件
	 * 在新通道建立时记录日志
	 *
	 * @param ctx 通道处理上下文
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.info("new connection build");
	}
}
