package com.zhb.cache;

import com.zhb.config.GlobalProperties;
import com.zhb.config.TopicInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * 缓存各个实例,没使用spring来管理bean
 */

public class CommonCache {

	@Getter
	@Setter
	public static GlobalProperties globalProperties = new GlobalProperties();

	@Getter
	@Setter
	public static TopicInfo topicInfo = new TopicInfo();


}
