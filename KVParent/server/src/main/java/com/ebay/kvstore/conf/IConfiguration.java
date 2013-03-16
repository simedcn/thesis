package com.ebay.kvstore.conf;

import java.util.Map.Entry;

public interface IConfiguration extends Iterable<Entry<Object, Object>> {

	public String get(String key);

	public void set(String key, Object value);

	public String get(String key, String defaultValue);

	public Integer getInt(String key, Integer defaultValue);

	public Integer getInt(String key);

	public Long getLong(String key, Long defaultValue);

	public Long getLong(String key);

	public IConfiguration merge(IConfiguration srcConf);

}
