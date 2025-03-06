package com.zhb.broker.slave;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created at 2024/7/13
 * @Description
 */
@ChannelHandler.Sharable
public class SlaveSyncServerHandler extends SimpleChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(SlaveSyncServerHandler.class);

    private EventBus eventBus;

    public SlaveSyncServerHandler(EventBus eventBus) {
        this.eventBus = eventBus;
        this.eventBus.init();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
        TcpMsg tcpMsg = (TcpMsg) msg;
        int code = tcpMsg.getCode();
        byte[] body = tcpMsg.getBody();
        Event event = null;
        if (BrokerEventCode.CREATE_TOPIC.getCode() == code) {
            CreateTopicReqDTO createTopicReqDTO = JSON.parseObject(body,CreateTopicReqDTO.class);
            CreateTopicEvent createTopicEvent= new CreateTopicEvent();
            createTopicEvent.setCreateTopicReqDTO(createTopicReqDTO);
            createTopicEvent.setMsgId(createTopicReqDTO.getMsgId());
            event = createTopicEvent;
            event.setChannelHandlerContext(channelHandlerContext);
            eventBus.publish(event);
        } else if (BrokerEventCode.PUSH_MSG.getCode() == code) {
            MessageDTO messageDTO = JSON.parseObject(body, MessageDTO.class);
            PushMsgEvent pushMsgEvent = new PushMsgEvent();
            pushMsgEvent.setMessageDTO(messageDTO);
            pushMsgEvent.setMsgId(messageDTO.getMsgId());
            logger.info("收到消息推送内容:{},message is {}", new String(messageDTO.getBody()), JSON.toJSONString(messageDTO));
            event = pushMsgEvent;
            event.setChannelHandlerContext(channelHandlerContext);
            eventBus.publish(event);
        } else if (BrokerResponseCode.START_SYNC_SUCCESS.getCode() == code) {
            StartSyncRespDTO startSyncRespDTO = JSON.parseObject(body, StartSyncRespDTO.class);
            SyncFuture syncFuture = BrokerServerSyncFutureManager.get(startSyncRespDTO.getMsgId());
            if (syncFuture != null) {
                syncFuture.setResponse(tcpMsg);
            }
        } else if(BrokerResponseCode.SLAVE_SYNC_RESP.getCode() == code) {
            SlaveSyncRespDTO slaveSyncRespDTO = JSON.parseObject(body, SlaveSyncRespDTO.class);
            SyncFuture syncFuture = BrokerServerSyncFutureManager.get(slaveSyncRespDTO.getMsgId());
            if (syncFuture != null) {
                syncFuture.setResponse(tcpMsg);
            }
        }
    }
}
