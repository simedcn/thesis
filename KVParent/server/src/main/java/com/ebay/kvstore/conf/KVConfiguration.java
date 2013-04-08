package com.ebay.kvstore.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.ebay.kvstore.ServerConstants;

public class KVConfiguration implements IConfiguration, IConfigurationKey {

	protected Properties properties;
	protected static Map<String, Integer> units;

	static {
		units = new HashMap<>();
		initUnits();
	}

	private static void initUnits() {
		units.put(Master_Gc_Check_Interval, ServerConstants.Second);
		units.put(Master_Checkpoint_Reserve_Days, ServerConstants.Day);
		units.put(Master_Checkpoint_Interval, ServerConstants.Day);
		units.put(Master_Unassign_Check_Interval, ServerConstants.Second);
		units.put(Master_Assign_Check_Interval, ServerConstants.Second);
		units.put(Master_Split_Check_Interval, ServerConstants.Second);
		units.put(Master_Wait_Dsjoin_Time, ServerConstants.Second);
		units.put(Dataserver_Region_Max, ServerConstants.MB);
		units.put(Dataserver_Region_Block_Size, ServerConstants.KB);
		units.put(Dataserver_Cache_Max, ServerConstants.KB);
		units.put(Dataserver_Buffer_Max, ServerConstants.KB);
		units.put(DataServer_Region_Reserve_Days, ServerConstants.Day);
		units.put(Heartbeat_Interval, ServerConstants.Second);
	}

	KVConfiguration(Properties p) {
		this.properties = p;
	}

	public int getUnit(String key) {
		Integer unit = units.get(key);
		if (unit != null) {
			return unit;
		} else {
			return 1;
		}
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
	public Double getDouble(String key) {
		return getDouble(key, 0.0);
	}

	@Override
	public Double getDouble(String key, Double defaultValue) {
		String value = properties.getProperty(key);
		int unit = getUnit(key);
		if (value == null) {
			if (defaultValue != null) {
				return defaultValue * unit;
			} else {
				return null;
			}
		} else {
			return Double.valueOf(value) * unit;
		}
	}

	@Override
	public Integer getInt(String key) {
		return getInt(key, null);
	}

	@Override
	public Integer getInt(String key, Integer defaultValue) {
		String value = properties.getProperty(key);
		int unit = getUnit(key);
		if (value == null) {
			if (defaultValue != null) {
				return defaultValue * unit;
			} else {
				return null;
			}
		} else {
			return Integer.valueOf(value) * unit;
		}
	}

	@Override
	public Long getLong(String key) {
		return getLong(key, null);
	}

	@Override
	public Long getLong(String key, Long defaultValue) {
		String value = properties.getProperty(key);
		int unit = getUnit(key);
		if (value == null) {
			if (defaultValue != null) {
				return defaultValue * unit;
			} else {
				return null;
			}
		} else {
			return Long.valueOf(value) * unit;
		}
	}

	@Override
	public Iterator<Entry<Object, Object>> iterator() {
		return properties.entrySet().iterator();
	}

	@Override
	public IConfiguration merge(IConfiguration srcConf) {
		for (Entry<Object, Object> e : srcConf) {
			properties.put(e.getKey(), e.getValue());
		}
		return this;
	}

	@Override
	public void set(String key, Object value) {
		properties.setProperty(key, value.toString());
	}

}
