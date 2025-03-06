package com.zhb.client.common;

import com.zhb.common.remote.BrokerNettyRemoteClient;
import com.zhb.common.transaction.TransactionListener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author idea
 * @Date: Created at 2024/7/16
 * @Description
 */
public class CommonCache {

    private static List<String> brokerAddressList;
    private static Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap = new ConcurrentHashMap<>();
    private static Map<String, TransactionListener> transactionListenerMap = new ConcurrentHashMap<>();

    public static Map<String, TransactionListener> getTransactionListenerMap() {
        return transactionListenerMap;
    }

    public static void setTransactionListenerMap(Map<String, TransactionListener> transactionListenerMap) {
        CommonCache.transactionListenerMap = transactionListenerMap;
    }

    public static Map<String, BrokerNettyRemoteClient> getBrokerNettyRemoteClientMap() {
        return brokerNettyRemoteClientMap;
    }

    public static void setBrokerNettyRemoteClientMap(Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap) {
        CommonCache.brokerNettyRemoteClientMap = brokerNettyRemoteClientMap;
    }

    public static List<String> getBrokerAddressList() {
        return brokerAddressList;
    }

    public static void setBrokerAddressList(List<String> brokerAddressList) {
        CommonCache.brokerAddressList = brokerAddressList;
    }
}
