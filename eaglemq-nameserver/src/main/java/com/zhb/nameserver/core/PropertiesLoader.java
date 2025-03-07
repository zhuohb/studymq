package com.zhb.nameserver.core;

import com.zhb.common.constants.BrokerConstants;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.MasterSlaveReplicationProperties;
import com.zhb.nameserver.common.NameserverProperties;
import com.zhb.nameserver.common.TraceReplicationProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * NameServer配置加载器
 * <p>
 * 负责从配置文件中加载NameServer运行所需的各项配置参数
 * 解析并构建配置对象，支持NameServer的各种运行模式（单机、主从复制、环形复制等）
 * <p>
 * 主要功能：
 * 1. 从nameserver.properties文件中读取配置项
 * 2. 解析基本配置信息（用户名、密码、端口）
 * 3. 构建复制模式相关配置（环形复制、主从复制）
 * 4. 将解析后的配置注入到全局缓存中，供其他组件使用
 */
public class PropertiesLoader {

	/**
	 * Java原生Properties对象，用于存储从配置文件中读取的原始键值对
	 */
	private Properties properties = new Properties();

	/**
	 * 加载NameServer配置
	 * <p>
	 * 从环境变量中获���EagleMQ安装目录，读取配置文件
	 * 解析各种配置项，构建配置对象并存储到全局缓存中
	 *
	 * @throws IOException 当配置文件读取失败时抛出异常
	 */
	public void loadProperties() throws IOException {
		// 从环境变量获取EagleMQ的安装目录
		String eagleMqHome = System.getenv(BrokerConstants.EAGLE_MQ_HOME);
		// 加载nameserver.properties配置文件
		properties.load(new FileInputStream(new File(eagleMqHome + "/config/nameserver.properties")));

		// 创建NameServer配置对象，并填充基本配置
		NameserverProperties nameserverProperties = new NameserverProperties();
		nameserverProperties.setNameserverPwd(getStr("nameserver.password"));
		nameserverProperties.setNameserverUser(getStr("nameserver.user"));
		nameserverProperties.setNameserverPort(getInt("nameserver.port"));
		nameserverProperties.setReplicationMode(getStrCanBeNull("nameserver.replication.mode"));

		// 创建并配置环形复制(Trace)相关属性
		TraceReplicationProperties traceReplicationProperties = new TraceReplicationProperties();
		traceReplicationProperties.setNextNode(getStrCanBeNull("nameserver.replication.trace.next.node"));
		traceReplicationProperties.setPort(getIntCanBeNull("nameserver.replication.trace.port"));
		nameserverProperties.setTraceReplicationProperties(traceReplicationProperties);

		// 创建并配置主从复制(Master-Slave)相关属性
		MasterSlaveReplicationProperties masterSlaveReplicationProperties = new MasterSlaveReplicationProperties();
		masterSlaveReplicationProperties.setMaster(getStrCanBeNull("nameserver.replication.master"));
		masterSlaveReplicationProperties.setRole(getStrCanBeNull("nameserver.replication.master.slave.role"));
		masterSlaveReplicationProperties.setType(getStrCanBeNull("nameserver.replication.master.slave.type"));
		masterSlaveReplicationProperties.setPort(getInt("nameserver.replication.port"));
		nameserverProperties.setMasterSlaveReplicationProperties(masterSlaveReplicationProperties);

		// 打印配置信息，方便调试
		nameserverProperties.print();

		// 将配置信息存入全局缓存，供其他组件使用
		CommonCache.setNameserverProperties(nameserverProperties);
	}

	/**
	 * 获取可为空的字符串配置项
	 * 当配置项不存在时，返回空字符串而非抛出异常
	 *
	 * @param key 配置项键名
	 * @return 配置项值或空字符串
	 */
	private String getStrCanBeNull(String key) {
		String value = properties.getProperty(key);
		if (value == null) {
			return "";
		}
		return value;
	}

	/**
	 * 获取必需的字符串配置项
	 * 当配置项不存在时，将抛出运行时异常
	 *
	 * @param key 配置项键名
	 * @return 配置项值
	 * @throws RuntimeException 当配置项不存在时抛出
	 */
	private String getStr(String key) {
		String value = properties.getProperty(key);
		if (value == null) {
			throw new RuntimeException("配置参数：" + key + "不存在");
		}
		return value;
	}

	/**
	 * 获取可为空的整数配置项
	 * 当配置项不存在时，返回null
	 *
	 * @param key 配置项键名
	 * @return 配置项整数值或null
	 */
	private Integer getIntCanBeNull(String key) {
		String intValue = getStr(key);
		if (intValue == null) {
			return null;
		}
		return Integer.valueOf(intValue);
	}

	/**
	 * 获取必需的整数配置项
	 * 将字符串配置转换为Integer类型
	 *
	 * @param key 配置项键名
	 * @return 配置项整数值
	 */
	private Integer getInt(String key) {
		return Integer.valueOf(getStr(key));
	}

	/**
	 * 根据键名获取配置项值
	 * 直接从Properties对象中获取原始值
	 *
	 * @param key 配置项键名
	 * @return 配置项值或null（如不存在）
	 */
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
