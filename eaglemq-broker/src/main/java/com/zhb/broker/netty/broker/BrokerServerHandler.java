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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @Author idea
 * @Date: Created in 18:05 2024/6/15
 * @Description
 */
@ChannelHandler.Sharable
public class BrokerServerHandler extends SimpleChannelInboundHandler {

	private static final Logger logger = LoggerFactory.getLogger(BrokerServerHandler.class);

	private EventBus eventBus;

	public BrokerServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		Event event = null;
		if (BrokerEventCode.PUSH_MSG.getCode() == code) {
			MessageDTO messageDTO = JSON.parseObject(body, MessageDTO.class);
			PushMsgEvent pushMsgEvent = new PushMsgEvent();
			pushMsgEvent.setMessageDTO(messageDTO);
			pushMsgEvent.setMsgId(messageDTO.getMsgId());
			logger.info("收到消息推送内容:{},message is {}", new String(messageDTO.getBody()), JSON.toJSONString(messageDTO));
			event = pushMsgEvent;
		} else if (BrokerEventCode.CONSUME_MSG.getCode() == code) {
			//这里需要设置一个消费者id
			ConsumeMsgReqDTO consumeMsgReqDTO = JSON.parseObject(body, ConsumeMsgReqDTO.class);
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
			consumeMsgReqDTO.setIp(inetSocketAddress.getHostString());
			consumeMsgReqDTO.setPort(inetSocketAddress.getPort());
			ConsumeMsgEvent consumeMsgEvent = new ConsumeMsgEvent();
			consumeMsgEvent.setConsumeMsgReqDTO(consumeMsgReqDTO);
			consumeMsgEvent.setMsgId(consumeMsgReqDTO.getMsgId());
			channelHandlerContext.attr(AttributeKey.valueOf("consumer-reqId")).set(consumeMsgReqDTO.getIp() + ":" + consumeMsgReqDTO.getPort());
			event = consumeMsgEvent;
		} else if (BrokerEventCode.CONSUME_SUCCESS_MSG.getCode() == code) {
			ConsumeMsgAckReqDTO consumeMsgAckReqDTO = JSON.parseObject(body, ConsumeMsgAckReqDTO.class);
			InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
			consumeMsgAckReqDTO.setIp(inetSocketAddress.getHostString());
			consumeMsgAckReqDTO.setPort(inetSocketAddress.getPort());
			ConsumeMsgAckEvent consumeMsgAckEvent = new ConsumeMsgAckEvent();
			consumeMsgAckEvent.setConsumeMsgAckReqDTO(consumeMsgAckReqDTO);
			consumeMsgAckEvent.setMsgId(consumeMsgAckReqDTO.getMsgId());
			event = consumeMsgAckEvent;
		} else if (BrokerEventCode.CONSUME_LATER_MSG.getCode() == code) {
			ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO = JSON.parseObject(body, ConsumeMsgRetryReqDTO.class);
			ConsumeMsgRetryEvent consumeMsgRetryEvent = new ConsumeMsgRetryEvent();
			consumeMsgRetryEvent.setMsgId(consumeMsgRetryReqDTO.getMsgId());
			consumeMsgRetryEvent.setConsumeMsgRetryReqDTO(consumeMsgRetryReqDTO);
			event = consumeMsgRetryEvent;
		} else if (BrokerEventCode.CREATE_TOPIC.getCode() == code) {
			CreateTopicReqDTO createTopicReqDTO = JSON.parseObject(body, CreateTopicReqDTO.class);
			CreateTopicEvent createTopicEvent = new CreateTopicEvent();
			createTopicEvent.setCreateTopicReqDTO(createTopicReqDTO);
			createTopicEvent.setMsgId(createTopicReqDTO.getMsgId());
			event = createTopicEvent;
		} else if (BrokerEventCode.START_SYNC_MSG.getCode() == code) {
			StartSyncReqDTO startSyncReqDTO = JSON.parseObject(body, StartSyncReqDTO.class);
			StartSyncEvent startSyncEvent = new StartSyncEvent();
			startSyncEvent.setMsgId(startSyncReqDTO.getMsgId());
			event = startSyncEvent;
		} else if (BrokerResponseCode.SLAVE_SYNC_RESP.getCode() == code) {
			SlaveSyncRespDTO slaveSyncRespDTO = JSON.parseObject(body, SlaveSyncRespDTO.class);
			SyncFuture slaveAckRsp = BrokerServerSyncFutureManager.get(slaveSyncRespDTO.getMsgId());
			slaveAckRsp.setResponse(slaveSyncRespDTO);
		}
		if (event != null) {
			event.setChannelHandlerContext(channelHandlerContext);
			eventBus.publish(event);
		}
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
		logger.error("error is :", cause);
	}


	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		//链接断开的时候，从重平衡池中移除
		Object reqId = ctx.attr(AttributeKey.valueOf("consumer-reqId")).get();
		if (reqId == null) {
			return;
		}
		CommonCache.getConsumerInstancePool().removeFromInstancePool(String.valueOf(reqId));
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("new connection build");
	}
}
