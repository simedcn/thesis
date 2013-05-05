package com.ebay.kvstore.server.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.ebay.kvstore.util.IKVConstants;

public class KVConfiguration implements IConfiguration, IConfigurationKey {

	protected Properties properties;
	protected static Map<String, Integer> units;

	static {
		units = new HashMap<>();
		initUnits();
	}

	private static void initUnits() {
		units.put(Master_Gc_Check_Interval, IKVConstants.Second);
		units.put(Master_Checkpoint_Reserve_Days, IKVConstants.Day);
		units.put(Master_Checkpoint_Interval, IKVConstants.Day);
		units.put(Master_Unassign_Check_Interval, IKVConstants.Second);
		units.put(Master_Assign_Check_Interval, IKVConstants.Second);
		units.put(Master_Split_Check_Interval, IKVConstants.Second);
		units.put(Master_Wait_Dsjoin_Time, IKVConstants.Second);
		units.put(Master_Merge_Check_Interval, IKVConstants.Second);
		units.put(Dataserver_Region_Max, IKVConstants.MB);
		units.put(Dataserver_Region_Block_Size, IKVConstants.KB);
		units.put(Dataserver_Cache_Max, IKVConstants.KB);
		units.put(Dataserver_Region_Buffer_Max, IKVConstants.KB);
		units.put(DataServer_Region_Reserve_Days, IKVConstants.Day);
		units.put(Heartbeat_Interval, IKVConstants.Second);
		units.put(Tmp_File_Reserve_Days, IKVConstants.Day);
		units.put(Dataserver_Region_Bloomfilter_Size, IKVConstants.KB);
	}

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
	public String[] getArray(String key) {
		String value = properties.getProperty(key);
		String[] values = null;
		if (value != null) {
			values = value.split(",");
		}
		return values;
	}

	@Override
	public Boolean getBoolean(String key) {
		return getBoolean(key, null);
	}

	@Override
	public Boolean getBoolean(String key, Boolean defaultValue) {
		String value = properties.getProperty(key);
		if (value == null) {
			return defaultValue;
		} else {
			return Boolean.valueOf(value);
		}

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
	public int getUnit(String key) {
		Integer unit = units.get(key);
		if (unit != null) {
			return unit;
		} else {
			return 1;
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
