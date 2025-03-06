package com.zhb.common.remote;

import com.zhb.common.cache.NameServerSyncFutureManager;

import java.util.concurrent.*;

/**
 * @Author idea
 * @Date: Created in 19:56 2024/6/10
 * @Description
 */
public class SyncFuture implements Future {

    /**
     * 远程rpc返回的数据内容
     */
    private Object response;
    private String msgId;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public Object getResponse() {
        return response;
    }

    public void setResponse(Object response) {
        this.response = response;
        countDownLatch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        countDownLatch.await();
        NameServerSyncFutureManager.remove(msgId);
        return response;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            countDownLatch.await(timeout,unit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            NameServerSyncFutureManager.remove(msgId);
        }
        return response;
    }
}
