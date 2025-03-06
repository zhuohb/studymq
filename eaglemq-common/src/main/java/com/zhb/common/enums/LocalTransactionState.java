package com.zhb.common.enums;

/**
 * @Author idea
 * @Date Created at 2024/8/18
 * @Description 本地事务执行状态
 */
public enum LocalTransactionState {

    COMMIT(0),
    ROLLBACK(1),
    UNKNOW(2),
    ;

    int code;

    LocalTransactionState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
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
