package com.zhb.client.async.event.model;

import com.zhb.common.dto.TxMessageCallbackReqDTO;
import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created at 2024/8/18
 * @Description
 */
public class TxMessageCallBackEvent extends Event {

    private TxMessageCallbackReqDTO txMessageCallbackReqDTO;

    public TxMessageCallbackReqDTO getTxMessageCallbackReqDTO() {
        return txMessageCallbackReqDTO;
    }

    public void setTxMessageCallbackReqDTO(TxMessageCallbackReqDTO txMessageCallbackReqDTO) {
        this.txMessageCallbackReqDTO = txMessageCallbackReqDTO;
    }
}
