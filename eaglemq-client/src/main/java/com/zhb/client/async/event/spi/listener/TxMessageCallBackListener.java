package com.zhb.client.async.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.client.async.event.model.TxMessageCallBackEvent;
import com.zhb.client.common.CommonCache;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.TxMessageCallbackReqDTO;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.enums.LocalTransactionState;
import com.zhb.common.enums.TxMessageFlagEnum;
import com.zhb.common.event.Listener;
import com.zhb.common.transaction.TransactionListener;
import com.zhb.common.utils.AssertUtils;

/**
 * @Author idea
 * @Date: Created at 2024/8/18
 * @Description 事务消息回调监听器
 */
public class TxMessageCallBackListener implements Listener<TxMessageCallBackEvent> {

    @Override
    public void onReceive(TxMessageCallBackEvent event) throws Exception {
        //提取出对应的listener，然后进行回调
        TxMessageCallbackReqDTO txMessageCallbackReqDTO = event.getTxMessageCallbackReqDTO();
        AssertUtils.isNotNull(txMessageCallbackReqDTO.getMessageDTO().getProducerId(),"producerId is null");
        AssertUtils.isNotNull(txMessageCallbackReqDTO.getMessageDTO().getMsgId(),"msgId is null");
        TransactionListener transactionListener = CommonCache.getTransactionListenerMap().get(txMessageCallbackReqDTO.getMessageDTO().getProducerId());
        LocalTransactionState localTransactionState = transactionListener.callBackHandler(txMessageCallbackReqDTO.getMessageDTO());
        MessageDTO messageDTO = txMessageCallbackReqDTO.getMessageDTO();
        if(LocalTransactionState.COMMIT == localTransactionState) {
            messageDTO.setTxFlag(TxMessageFlagEnum.REMAIN_HALF_ACK.getCode());
            messageDTO.setLocalTxState(LocalTransactionState.COMMIT.getCode());
            TcpMsg remainHalfAckMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
            event.getChannelHandlerContext().writeAndFlush(remainHalfAckMsg);
            System.out.println("commit callback");
        } else if(LocalTransactionState.ROLLBACK == localTransactionState) {
            messageDTO.setTxFlag(TxMessageFlagEnum.REMAIN_HALF_ACK.getCode());
            messageDTO.setLocalTxState(LocalTransactionState.ROLLBACK.getCode());
            TcpMsg remainHalfAckMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
            event.getChannelHandlerContext().writeAndFlush(remainHalfAckMsg);
            System.out.println("rollback callback");
        }
        //unknow状态不处理
    }
}
