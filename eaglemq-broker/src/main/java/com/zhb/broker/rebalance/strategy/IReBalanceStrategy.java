package com.zhb.broker.rebalance.strategy;

/**
 * @Author idea
 * @Date: Created in 14:05 2024/6/23
 * @Description
 */
public interface IReBalanceStrategy {

    /**
     * 根据策略执行重分配
     *
     * @param reBalanceInfo
     */
    void doReBalance(ReBalanceInfo reBalanceInfo);
}
