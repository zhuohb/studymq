package com.zhb.common.dto;

import com.zhb.common.enums.SendMessageToBrokerResponseStatus;

/**
 * @Author idea
 * @Date: Created in 20:08 2024/6/15
 * @Description
 */
public class SendMessageToBrokerResponseDTO extends BaseBrokerRemoteDTO{

    /**
     * 发送消息的结果状态
     *
     * @see SendMessageToBrokerResponseStatus
     */
    private int status;

    private String desc;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
