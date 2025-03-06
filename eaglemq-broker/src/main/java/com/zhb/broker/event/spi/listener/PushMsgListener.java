package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.PushMsgEvent;
import com.zhb.broker.model.TxMessageAckModel;
import com.zhb.broker.timewheel.DelayMessageDTO;
import com.zhb.broker.timewheel.SlotStoreTypeEnum;
import com.zhb.common.cache.BrokerServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.SendMessageToBrokerResponseDTO;
import com.zhb.common.dto.SlaveSyncRespDTO;
import com.zhb.common.dto.TxMessageDTO;
import com.zhb.common.enums.*;
import com.zhb.common.event.Listener;
import com.zhb.common.remote.SyncFuture;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author idea
 * @Date: Created in 09:46 2024/6/16
 * @Description
 */
public class PushMsgListener implements Listener<PushMsgEvent> {

    private static final Logger log = LoggerFactory.getLogger(PushMsgListener.class);

    @Override
    public void onReceive(PushMsgEvent event) throws IOException {
        //消息写入commitLog
        MessageDTO messageDTO = event.getMessageDTO();
        boolean isDelayMsg = messageDTO.getDelay() > 0;
        boolean isHalfMsg = messageDTO.getTxFlag() == TxMessageFlagEnum.HALF_MSG.getCode();
        boolean isRemainHalfAck = messageDTO.getTxFlag() == TxMessageFlagEnum.REMAIN_HALF_ACK.getCode();
        if(isDelayMsg) {
            this.appendDelayMsgHandler(messageDTO,event);
        } else if (isHalfMsg) {
            this.halfMsgHandler(messageDTO,event);
        } else if (isRemainHalfAck) {
            this.remainHalfMsgAckHandler(messageDTO,event);
        } else {
            this.appendDefaultMsgHandler(messageDTO,event);
        }
    }

    private void remainHalfMsgAckHandler(MessageDTO messageDTO,PushMsgEvent event) throws IOException {
        LocalTransactionState localTransactionState = LocalTransactionState.of(messageDTO.getLocalTxState());
        if(localTransactionState == LocalTransactionState.COMMIT) {
            CommonCache.getTxMessageAckModelMap().remove(messageDTO.getMsgId());
            CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO);
            log.info("收到事务消息的commit请求");
        } else if (localTransactionState == LocalTransactionState.ROLLBACK) {
            CommonCache.getTxMessageAckModelMap().remove(messageDTO.getMsgId());
            log.info("收到事务消息的rollback请求");
        }
        //告诉客户端写入事务消息成功
        SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
        sendMsgResp.setMsgId(messageDTO.getMsgId());
        sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
        sendMsgResp.setDesc("send tx remain ack msg success");
        TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.REMAIN_ACK_MSG_SEND_SUCCESS.getCode(), JSON.toJSONBytes(sendMsgResp));
        event.getChannelHandlerContext().writeAndFlush(responseMsg);
    }

    private void halfMsgHandler(MessageDTO messageDTO, PushMsgEvent event) {
        TxMessageAckModel txMessageAckModel = new TxMessageAckModel();
        txMessageAckModel.setMessageDTO(messageDTO);
        txMessageAckModel.setChannelHandlerContext(event.getChannelHandlerContext());
        txMessageAckModel.setFirstSendTime(System.currentTimeMillis());
        CommonCache.getTxMessageAckModelMap().put(messageDTO.getMsgId(),txMessageAckModel);
        //时间轮推送 -》
        TxMessageDTO txMessageDTO = new TxMessageDTO();
        txMessageDTO.setMsgId(messageDTO.getMsgId());
        long currentTime = System.currentTimeMillis();
        DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
        delayMessageDTO.setData(txMessageDTO);
        delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.TX_MESSAGE_DTO);
        delayMessageDTO.setNextExecuteTime(currentTime + 3 *1000);
        delayMessageDTO.setDelay(3);
        CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
        //告诉客户端写入事务消息成功
        SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
        sendMsgResp.setMsgId(messageDTO.getMsgId());
        sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
        sendMsgResp.setDesc("send tx half msg success");
        TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.HALF_MSG_SEND_SUCCESS.getCode(), JSON.toJSONBytes(sendMsgResp));
        event.getChannelHandlerContext().writeAndFlush(responseMsg);
    }

    /**
     * 普通commitLog消息追加写入
     * @param messageDTO
     * @param event
     */
    private void appendDefaultMsgHandler(MessageDTO messageDTO,PushMsgEvent event) throws IOException {
        CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO,event);
    }


    /**
     * 延迟消息追加写入
     *
     * @param messageDTO
     * @param event
     */
    private void appendDelayMsgHandler(MessageDTO messageDTO , PushMsgEvent event) throws IOException {
        int delaySeconds = messageDTO.getDelay();
        AssertUtils.isTrue(delaySeconds <= 3600 , "too large delay seconds");
        DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
        delayMessageDTO.setDelay(messageDTO.getDelay());
        delayMessageDTO.setData(messageDTO);
        delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.DELAY_MESSAGE_DTO);
        delayMessageDTO.setNextExecuteTime(System.currentTimeMillis() + delaySeconds * 1000);
        //延迟消息的下入逻辑
        CommonCache.getTimeWheelModelManager().add(delayMessageDTO);
        //持久化
        MessageDTO delayMessage = new MessageDTO();
        delayMessage.setBody(JSON.toJSONBytes(delayMessageDTO));
        delayMessage.setTopic("delay_queue");
        delayMessage.setQueueId(0);
        delayMessage.setSendWay(MessageSendWay.ASYNC.getCode());
        CommonCache.getCommitLogAppendHandler().appendMsg(delayMessage,event);
        SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
        sendMsgResp.setMsgId(messageDTO.getMsgId());
        sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
        sendMsgResp.setDesc("send delay msg success");
        TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.SEND_MSG_RESP.getCode(), JSON.toJSONBytes(sendMsgResp));
        event.getChannelHandlerContext().writeAndFlush(responseMsg);
    }
}
