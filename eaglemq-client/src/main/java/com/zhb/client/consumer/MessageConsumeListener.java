package com.zhb.client.consumer;


import java.util.List;

/**
 * @Author idea
 * @Date: Created in 11:05 2024/6/16
 * @Description 数据消费监听器
 */
public interface MessageConsumeListener {


    /**
     * 默认的消费处理函数
     *
     * @param consumeMessages
     * @return
     */
    ConsumeResult consume(List<ConsumeMessage> consumeMessages) throws InterruptedException;
}
