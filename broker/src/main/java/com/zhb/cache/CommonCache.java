package com.zhb.cache;

import com.zhb.config.GlobalProperties;
import com.zhb.model.TopicModel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 缓存各个实例,没使用spring来管理bean
 */

public class CommonCache {

	@Getter
	@Setter
	public static GlobalProperties globalProperties = new GlobalProperties();

	@Getter
	@Setter
	public static Map<String, TopicModel> topicModelMap = new HashMap<>();


}
