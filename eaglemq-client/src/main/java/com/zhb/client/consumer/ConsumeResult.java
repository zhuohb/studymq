package com.zhb.client.consumer;

import com.zhb.common.enums.ConsumeResultStatus;

/**
 * @Author idea
 * @Date: Created in 11:06 2024/6/16
 * @Description
 */
public class ConsumeResult {

    /**
     * 消费结果
     */
    private int consumeResultStatus;

    public int getConsumeResultStatus() {
        return consumeResultStatus;
    }

    public void setConsumeResultStatus(int consumeResultStatus) {
        this.consumeResultStatus = consumeResultStatus;
    }

    public ConsumeResult(int consumeResultStatus) {
        this.consumeResultStatus = consumeResultStatus;
    }

    public static ConsumeResult CONSUME_SUCCESS() {
        return new ConsumeResult(ConsumeResultStatus.CONSUME_SUCCESS.getCode());
    }

    public static ConsumeResult CONSUME_LATER() {
        return new ConsumeResult(ConsumeResultStatus.CONSUME_LATER.getCode());
    }
}
