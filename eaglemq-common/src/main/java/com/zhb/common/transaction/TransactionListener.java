package com.zhb.common.transaction;

import com.zhb.common.dto.MessageDTO;
import com.zhb.common.enums.LocalTransactionState;


public interface TransactionListener {

	/**
	 * 执行本地事务逻辑处理的回调函数
	 *
	 * @param messageDTO
	 * @return
	 */
	LocalTransactionState executeLocalTransaction(final MessageDTO messageDTO);

	/**
	 * 事务消息从broker回调到本地的回调接口
	 *
	 * @param messageDTO
	 * @return
	 */
	LocalTransactionState callBackHandler(final MessageDTO messageDTO);
}
