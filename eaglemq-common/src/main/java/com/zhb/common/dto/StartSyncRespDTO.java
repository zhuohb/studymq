package com.zhb.common.dto;


public class StartSyncRespDTO extends BaseBrokerRemoteDTO{

    private boolean isSuccess;

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }
}
