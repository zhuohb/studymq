package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * broker-slave同步成功响应DTO
 */
@Setter
@Getter
public class SlaveSyncRespDTO extends BaseBrokerRemoteDTO {

	private boolean syncSuccess;

}
