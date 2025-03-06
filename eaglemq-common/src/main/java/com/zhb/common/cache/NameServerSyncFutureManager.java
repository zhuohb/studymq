package com.zhb.common.cache;

import com.zhb.common.remote.SyncFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author idea
 * @Date: Created in 20:00 2024/6/10
 * @Description
 */
public class NameServerSyncFutureManager {

    private static Map<String, SyncFuture> syncFutureMap = new ConcurrentHashMap<>();

    public static void put(String key, SyncFuture syncFuture) {
        syncFutureMap.put(key, syncFuture);
    }

    public static SyncFuture get(String key) {
        return syncFutureMap.get(key);
    }

    public static void remove(String key) {
        syncFutureMap.remove(key);
    }
}
