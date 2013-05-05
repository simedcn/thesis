package com.ebay.kvstore.server.conf;

import java.util.Map.Entry;

public interface IConfiguration extends Iterable<Entry<Object, Object>> {

	public String get(String key);

	public String get(String key, String defaultValue);

	public String[] getArray(String key);

	public Boolean getBoolean(String key);

	public Boolean getBoolean(String key, Boolean defaultValue);

	public Double getDouble(String key);

	public Double getDouble(String key, Double defaultValue);

	public Integer getInt(String key);

	public Integer getInt(String key, Integer defaultValue);

	public Long getLong(String key);

	public Long getLong(String key, Long defaultValue);

	public int getUnit(String key);

	public IConfiguration merge(IConfiguration srcConf);

	public void set(String key, Object value);

}
