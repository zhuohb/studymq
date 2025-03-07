package com.zhb.nameserver.utils;

import com.zhb.nameserver.common.CommonCache;

/**
 * NameServer工具类
 * <p>
 * 该类提供了一组与NameServer组件相关的工具方法，用于辅助NameServer的核心功能实现。
 * 包含身份验证、配置检查和其他通用辅助功能。
 * <p>
 * 工具类设计为静态方法集合，便于在系统各组件中调用，无需创建实例。
 * 主要用于分离通用逻辑，提高代码复用性和可维护性。
 * <p>
 * 主要功能：
 * - 用户身份验证
 * - 系统参数检查
 * - 配置信息处理
 */
public class NameserverUtils {

	/**
	 * 验证用户凭据
	 * <p>
	 * 该方法检查提供的用户名和密码是否与系统配置的凭据匹配。
	 * 用于NameServer中的安全验证，特别是在节点间通信和管理操作时。
	 * <p>
	 * 验证逻辑：
	 * 1. 从CommonCache中获取配置的用户名和密码
	 * 2. 与传入的凭据进行精确比对（大小写敏感）
	 * 3. 只有当用户名和密码都匹配时才返回true
	 * <p>
	 * 安全考量：
	 * - 使用字符串对象的equals方法确保安全比较
	 * - 两项凭据都必须匹配，缺一不可
	 *
	 * @param user 要验证的用户名
	 * @param password 要验证的密码
	 * @return 验证结果，如果凭据匹配返回true，否则返回false
	 */
	public static boolean isVerify(String user, String password) {
		String rightUser = CommonCache.getNameserverProperties().getNameserverUser();
		String rightPassword = CommonCache.getNameserverProperties().getNameserverPwd();
		if (!rightUser.equals(user) || !rightPassword.equals(password)) {
			return false;
		}
		return true;
	}
}
