package com.zhb;


import com.zhb.cache.CommonCache;
import com.zhb.config.EagleMqTopicLoader;
import com.zhb.config.GlobalPropertiesLoader;
import com.zhb.core.CommitLogAppendHandler;
import com.zhb.model.EagleMqTopicModel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @Author idea
 * @Date: Created in 22:57 2024/3/26
 * @Description
 */
public class BrokerStartUp {

    private static GlobalPropertiesLoader globalPropertiesLoader;
    private static EagleMqTopicLoader eagleMqTopicLoader;
    private static CommitLogAppendHandler commitLogAppendHandler;

    /**
     * 初始化配置逻辑
     */
    private static void initProperties() throws IOException {
        globalPropertiesLoader = new GlobalPropertiesLoader();
        globalPropertiesLoader.loadProperties();
        eagleMqTopicLoader = new EagleMqTopicLoader();
        eagleMqTopicLoader.loadProperties();
        eagleMqTopicLoader.startRefreshEagleMqTopicInfoTask();
        commitLogAppendHandler = new CommitLogAppendHandler();

        for (EagleMqTopicModel eagleMqTopicModel : CommonCache.getEagleMqTopicModelMap().values()) {
            String topicName = eagleMqTopicModel.getTopic();
            commitLogAppendHandler.prepareMMapLoading(topicName);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //加载配置 ，缓存对象的生成
        initProperties();
        //模拟初始化文件映射
        String topic = "order_cancel_topic";
        for(int i=0;i<10;i++){
            commitLogAppendHandler.appendMsg(topic,("this is content " + i).getBytes());
            System.out.println("写入数据");
            TimeUnit.SECONDS.sleep(5);
        }
        commitLogAppendHandler.readMsg(topic);
    }
}
