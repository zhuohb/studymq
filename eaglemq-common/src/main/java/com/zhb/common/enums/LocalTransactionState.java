package com.zhb.common.enums;

import lombok.Getter;

/**
 * 本地事务执行状态
 */
@Getter
public enum LocalTransactionState {

	COMMIT(0),
	ROLLBACK(1),
	UNKNOW(2),
	;

	private final int code;

	LocalTransactionState(int code) {
		this.code = code;
	}

	public static LocalTransactionState of(int code) {
		for (LocalTransactionState state : LocalTransactionState.values()) {
			if (state.code == code) {
				return state;
			}
		}
		return null;
	}
}
