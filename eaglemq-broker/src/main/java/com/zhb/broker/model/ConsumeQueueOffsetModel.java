package com.zhb.broker.model;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 08:12 2024/4/16
 * @Description
 */
public class ConsumeQueueOffsetModel {

    private OffsetTable offsetTable = new OffsetTable();

    public static class OffsetTable {
        private Map<String,ConsumerGroupDetail> topicConsumerGroupDetail = new HashMap<>();

        public Map<String, ConsumerGroupDetail> getTopicConsumerGroupDetail() {
            return topicConsumerGroupDetail;
        }

        public void setTopicConsumerGroupDetail(Map<String, ConsumerGroupDetail> topicConsumerGroupDetail) {
            this.topicConsumerGroupDetail = topicConsumerGroupDetail;
        }
    }

    public static class ConsumerGroupDetail {
        private Map<String,Map<String,String>> consumerGroupDetailMap = new HashMap<>();;

        public Map<String, Map<String, String>> getConsumerGroupDetailMap() {
            return consumerGroupDetailMap;
        }

        public void setConsumerGroupDetailMap(Map<String, Map<String, String>> consumerGroupDetailMap) {
            this.consumerGroupDetailMap = consumerGroupDetailMap;
        }
    }

    public OffsetTable getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(OffsetTable offsetTable) {
        this.offsetTable = offsetTable;
    }
}
