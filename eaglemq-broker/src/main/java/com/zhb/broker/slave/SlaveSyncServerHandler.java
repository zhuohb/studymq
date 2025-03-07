package com.zhb.broker.slave;

import com.alibaba.fastjson2.JSON;
import com.zhb.broker.event.model.CreateTopicEvent;
import com.zhb.broker.event.model.PushMsgEvent;
import com.zhb.common.cache.BrokerServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.CreateTopicReqDTO;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.SlaveSyncRespDTO;
import com.zhb.common.dto.StartSyncRespDTO;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;
import com.zhb.common.remote.SyncFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 从节点同步服务器处理器
 * 负责处理从主节点接收到的同步消息
 * 将网络消息转换为事件并发布到事件总线上，包括主题创建、消息推送等操作
 */
@Slf4j
@ChannelHandler.Sharable
public class SlaveSyncServerHandler extends SimpleChannelInboundHandler {

	/**
	 * 事件总线
	 * 用于发布和处理同步事件
	 */
	private EventBus eventBus;

	/**
	 * 构造函数
	 * 初始化处理器，设置并初始化事件总线
	 *
	 * @param eventBus 事件总线实例，用于处理和分发同步事件
	 */
	public SlaveSyncServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 通道读取方法
	 * 处理从主节点接收到的各类同步消息
	 * 根据消息类型转换为相应的事件或处理响应
	 *
	 * @param channelHandlerContext 通道处理上下文
	 * @param msg                   接收到的消息对象
	 * @throws Exception 处理过程中可能抛出的异常
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		Event event = null;
		if (BrokerEventCode.CREATE_TOPIC.getCode() == code) {
			// 处理创建主题消息
			CreateTopicReqDTO createTopicReqDTO = JSON.parseObject(body, CreateTopicReqDTO.class);
			CreateTopicEvent createTopicEvent = new CreateTopicEvent();
			createTopicEvent.setCreateTopicReqDTO(createTopicReqDTO);
			createTopicEvent.setMsgId(createTopicReqDTO.getMsgId());
			event = createTopicEvent;
			event.setChannelHandlerContext(channelHandlerContext);
			eventBus.publish(event);
		} else if (BrokerEventCode.PUSH_MSG.getCode() == code) {
			// 处理消息推送
			MessageDTO messageDTO = JSON.parseObject(body, MessageDTO.class);
			PushMsgEvent pushMsgEvent = new PushMsgEvent();
			pushMsgEvent.setMessageDTO(messageDTO);
			pushMsgEvent.setMsgId(messageDTO.getMsgId());
			log.info("收到消息推送内容:{},message is {}", new String(messageDTO.getBody()), JSON.toJSONString(messageDTO));
			event = pushMsgEvent;
			event.setChannelHandlerContext(channelHandlerContext);
			eventBus.publish(event);
		} else if (BrokerResponseCode.START_SYNC_SUCCESS.getCode() == code) {
			// 处理开始同步成功响应
			StartSyncRespDTO startSyncRespDTO = JSON.parseObject(body, StartSyncRespDTO.class);
			SyncFuture syncFuture = BrokerServerSyncFutureManager.get(startSyncRespDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		} else if (BrokerResponseCode.SLAVE_SYNC_RESP.getCode() == code) {
			// 处理从节点同步响应
			SlaveSyncRespDTO slaveSyncRespDTO = JSON.parseObject(body, SlaveSyncRespDTO.class);
			SyncFuture syncFuture = BrokerServerSyncFutureManager.get(slaveSyncRespDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		}
	}
}
