package com.ebay.kvstore.server.data.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;
import com.ebay.kvstore.util.KeyValueUtil;
import com.ebay.kvstore.util.RegionUtil;

public abstract class BaseStoreEngine implements IStoreEngine {

	private static Logger logger = LoggerFactory.getLogger(BaseStoreEngine.class);

	protected KeyValueCache cache;

	protected int cacheLimit;

	protected List<Region> regions;

	protected Set<IStoreEngineListener> listeners;

	protected IConfiguration conf;

	protected Address addr;

	public BaseStoreEngine(IConfiguration conf, Region... regions) {
		this.conf = conf;
		cacheLimit = conf.getInt(IConfigurationKey.Dataserver_Cache_Max);
		this.regions = new ArrayList<>();
		listeners = new HashSet<>();
		this.addr = Address.parse(conf.get(IConfigurationKey.Dataserver_Addr));
		cache = KeyValueCache.forCache(cacheLimit,
				conf.get(IConfigurationKey.Dataserver_Cache_Replacement_Policy));
		Collections.addAll(this.regions, regions);
		Collections.sort(this.regions);
	}

	@Override
	public Region[] getAllRegions() {
		return regions.toArray(new Region[regions.size()]);
	}

	@Override
	public int getCacheLimit() {
		return cacheLimit;
	}

	public void onLoad(Region region) {
		for (IStoreEngineListener listener : listeners) {
			listener.onLoad(region);
		}
	}

	@Override
	public void registerListener(IStoreEngineListener listener) {
		listeners.add(listener);
	}

	@Override
	public void setCacheLimit(int limit) {
		this.cacheLimit = limit;
		cache.setLimit(cacheLimit);
	}

	@Override
	public void unregisterListener(IStoreEngineListener listener) {
		listeners.remove(listener);
	}

	void onDelete(byte[] key) {
		Region region = getKeyRegion(key);
		for (IStoreEngineListener listener : listeners) {
			try {
				listener.onDelete(region, key);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	void onGet(byte[] key, Value value) {
		Region region = getKeyRegion(key);
		for (IStoreEngineListener listener : listeners) {
			try {
				listener.onGet(region, key, value);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	void onIncr(byte[] key, int value) {
		Region region = getKeyRegion(key);
		for (IStoreEngineListener listener : listeners) {
			try {
				listener.onIncr(region, key, value);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	void onSet(byte[] key, byte[] value) {
		Region region = getKeyRegion(key);
		for (IStoreEngineListener listener : listeners) {
			try {
				listener.onSet(region, key, value);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	void onSplit(Region oldRegion, Region newRegion) {
		for (IStoreEngineListener listener : listeners) {
			try {
				listener.onSplit(oldRegion, newRegion);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	protected synchronized boolean addRegion(Region region) {
		if (regions.contains(region)) {
			return false;
		}
		regions.add(region);
		Collections.sort(regions);
		return true;
	}

	protected Region checkKeyRegion(byte[] key) throws InvalidKeyException {
		Region region = RegionUtil.search(regions, key);
		if (region == null) {
			throw new InvalidKeyException("The given key:" + Arrays.toString(key)
					+ " is not served in this data server!");
		}
		return region;
	}

	protected boolean checkRegionConsistent(Region region1, Region region2) {
		byte[] key1 = null;
		byte[] key2 = null;
		int result = region1.compareTo(region2);
		if (result < 0) {
			key1 = region1.getEnd();
			key2 = region2.getStart();
		} else if (result > 0) {
			key1 = region2.getEnd();
			key2 = region1.getStart();
		} else {
			return false;
		}
		return Arrays.equals(KeyValueUtil.nextKey(key1), key2);
	}

	protected Region getKeyRegion(byte[] key) {
		return RegionUtil.search(regions, key);
	}

	protected Region getKeyRegion(List<Region> regions, byte[] key) {
		return RegionUtil.search(regions, key);
	}

	protected Region getRegionById(int regionId) {
		for (Region region : regions) {
			if (region.getRegionId() == regionId) {
				return region;
			}
		}
		return null;
	}

	protected KeyValue incr(KeyValue kv, byte[] key, int initValue, int incremental, int ttl) {
		long expire = KeyValueUtil.getExpireTime(ttl);
		Value value;
		if (kv == null || !KeyValueUtil.isAlive(kv.getValue())) {
			byte[] bytes = KeyValueUtil.intToBytes(initValue + incremental);
			value = new Value(bytes, expire);
			kv = new KeyValue(key, value);
		} else {
			value = kv.getValue();
			value.incr(incremental);
			value.setExpire(expire);
		}
		return kv;
	}

	protected synchronized Region removeRegion(int regionId) {
		Iterator<Region> it = regions.iterator();
		Region region = null;
		while (it.hasNext()) {
			region = it.next();
			if (region.getRegionId() == regionId) {
				it.remove();
				break;
			}
		}
		return region;
	}
}
