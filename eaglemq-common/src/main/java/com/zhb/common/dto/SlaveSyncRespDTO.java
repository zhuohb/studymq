package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created at 2024/7/14
 * @Description broker-slave同步成功响应DTO
 */
public class SlaveSyncRespDTO extends BaseBrokerRemoteDTO{

    private boolean syncSuccess;

    public boolean isSyncSuccess() {
        return syncSuccess;
    }

    public void setSyncSuccess(boolean syncSuccess) {
        this.syncSuccess = syncSuccess;
    }
}
