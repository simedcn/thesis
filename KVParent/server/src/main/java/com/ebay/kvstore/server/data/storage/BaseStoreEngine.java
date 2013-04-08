package com.ebay.kvstore.server.data.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.ebay.kvstore.RegionUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public abstract class BaseStoreEngine implements IStoreEngine {

	protected KeyValueCache cache;

	protected int cacheLimit;

	protected List<Region> regions;

	protected Set<IStoreListener> listeners;

	protected IConfiguration conf;

	protected Address addr;

	public BaseStoreEngine(IConfiguration conf, Region... regions) {
		this.conf = conf;
		cacheLimit = conf.getInt(IConfigurationKey.Dataserver_Cache_Max);
		this.regions = new ArrayList<>();
		listeners = new HashSet<>();
		this.addr = Address.parse(conf.get(IConfigurationKey.Dataserver_Addr));
		// TODO:add configuration for
		cache = KeyValueCache.forCache(cacheLimit,
				conf.get(IConfigurationKey.Dataserver_Cache_Replacement_Policy));
		Collections.addAll(this.regions, regions);
		Collections.sort(this.regions);
	}

	@Override
	public int getCacheLimit() {
		return cacheLimit;
	}

	@Override
	public List<Region> getRegions() {
		return regions;
	}

	public void onLoad(Region region) {
		for (IStoreListener listener : listeners) {
			listener.onLoad(region);
		}
	}

	@Override
	public void registerListener(IStoreListener listener) {
		listeners.add(listener);
	}

	@Override
	public void setCacheLimit(int limit) {
		this.cacheLimit = limit;
		cache.setLimit(cacheLimit);
	}

	@Override
	public void unregisterListener(IStoreListener listener) {
		listeners.remove(listener);
	}

	void onDelete(byte[] key) {
		Region region = getKeyRegion(key);
		for (IStoreListener listener : listeners) {
			listener.onDelete(region, key);
		}
	}

	void onGet(byte[] key, Value value) {
		Region region = getKeyRegion(key);
		for (IStoreListener listener : listeners) {
			listener.onGet(region, key, value);
		}
	}

	void onIncr(byte[] key, int value) {
		Region region = getKeyRegion(key);
		for (IStoreListener listener : listeners) {
			listener.onIncr(region, key, value);
		}
	}

	void onSet(byte[] key, byte[] value) {
		Region region = getKeyRegion(key);
		for (IStoreListener listener : listeners) {
			listener.onSet(region, key, value);
		}
	}

	void onSplit(Region oldRegion, Region newRegion) {
		for (IStoreListener listener : listeners) {
			listener.onSplit(oldRegion, newRegion);
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
