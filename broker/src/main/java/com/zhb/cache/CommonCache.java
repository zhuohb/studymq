package com.zhb.cache;

import com.zhb.config.GlobalProperties;
import com.zhb.model.EagleMqTopicModel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 统一缓存对象
 */

public class CommonCache {

	@Getter
	@Setter
	public static GlobalProperties globalProperties = new GlobalProperties();

	@Getter
	@Setter
	public static List<EagleMqTopicModel> eagleMqTopicModelList = new ArrayList<>();

	public static Map<String, EagleMqTopicModel> getEagleMqTopicModelMap() {
		return eagleMqTopicModelList.stream().collect(Collectors.toMap(EagleMqTopicModel::getTopic, item -> item));
	}
}
