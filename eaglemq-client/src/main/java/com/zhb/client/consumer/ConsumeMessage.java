package com.zhb.client.consumer;

import com.zhb.common.dto.ConsumeMsgCommitLogDTO;

/**
 * @Author idea
 * @Date: Created in 11:07 2024/6/16
 * @Description
 */
public class ConsumeMessage {

    private int queueId;

    private ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO;

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public ConsumeMsgCommitLogDTO getConsumeMsgCommitLogDTO() {
        return consumeMsgCommitLogDTO;
    }

    public void setConsumeMsgCommitLogDTO(ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO) {
        this.consumeMsgCommitLogDTO = consumeMsgCommitLogDTO;
    }
}
