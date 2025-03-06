package com.zhb.client.consumer;

import java.util.List;

/**
 * @Author idea
 * @Date: Created in 11:09 2024/6/16
 * @Description 多线程消费监听器
 */
public class ConcurrentMessageConsumeListener implements MessageConsumeListener{

    @Override
    public ConsumeResult consume(List<ConsumeMessage> consumeMessages) {
        return null;
    }
}
