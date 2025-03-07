package com.zhb.broker.rebalance.strategy;


public interface IReBalanceStrategy {

	/**
	 * 根据策略执行重分配
	 *
	 * @param reBalanceInfo
	 */
	void doReBalance(ReBalanceInfo reBalanceInfo);
}
