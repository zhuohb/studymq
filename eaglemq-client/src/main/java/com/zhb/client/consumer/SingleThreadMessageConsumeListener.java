package com.zhb.client.consumer;

import java.util.List;

/**
 * @Author idea
 * @Date: Created in 11:08 2024/6/16
 * @Description 单线程数据消费监听器
 */
public class SingleThreadMessageConsumeListener implements MessageConsumeListener{

    @Override
    public ConsumeResult consume(List<ConsumeMessage> consumeMessages) {
        return null;
    }
}
