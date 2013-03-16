package com.ebay.kvstore.conf;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

public class KVConfiguration implements IConfiguration {

	protected Properties properties;

	KVConfiguration(Properties p) {
		this.properties = p;
	}

	@Override
	public String get(String key) {
		return get(key, null);
	}

	@Override
	public String get(String name, String defaultValue) {
		return properties.getProperty(name, defaultValue);
	}

	@Override
	public Integer getInt(String key, Integer defaultValue) {
		String value = properties.getProperty(key);
		if (value == null) {
			return defaultValue;
		} else {
			return Integer.valueOf(value);
		}
	}

	@Override
	public Integer getInt(String key) {
		return getInt(key, null);
	}

	@Override
	public Long getLong(String key, Long defaultValue) {
		String value = properties.getProperty(key);
		if (value == null) {
			return defaultValue;
		} else {
			return Long.valueOf(value);
		}
	}

	@Override
	public Long getLong(String key) {
		return getLong(key, null);
	}

	@Override
	public IConfiguration merge(IConfiguration srcConf) {
		for (Entry<Object, Object> e : srcConf) {
			properties.put(e.getKey(), e.getValue());
		}
		return this;
	}

	@Override
	public Iterator<Entry<Object, Object>> iterator() {
		return properties.entrySet().iterator();
	}

	@Override
	public void set(String key, Object value) {
		properties.setProperty(key, value.toString());
	}

}
