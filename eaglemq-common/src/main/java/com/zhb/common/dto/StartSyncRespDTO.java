package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created at 2024/7/10
 * @Description
 */
public class StartSyncRespDTO extends BaseBrokerRemoteDTO{

    private boolean isSuccess;

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }
}
