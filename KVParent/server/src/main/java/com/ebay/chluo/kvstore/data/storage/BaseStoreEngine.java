package com.ebay.chluo.kvstore.data.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ebay.chluo.kvstore.RegionUtil;
import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

public abstract class BaseStoreEngine implements IStoreEngine {

	protected KeyValueCache cache;

	protected int cacheLimit;

	protected List<Region> regions;

	protected Set<IStoreListener> listeners;

	public BaseStoreEngine(int limit) {
		cacheLimit = limit;
		regions = new ArrayList<>();
		listeners = new HashSet<>();
		// TODO:add configuration for
		cache = new KeyValueCache(cacheLimit, null);
	}

	@Override
	public List<Region> getRegions() {
		return regions;
	}

	@Override
	public void setCacheLimit(int limit) {
		this.cacheLimit = limit;
	}

	@Override
	public int getCacheLimit() {
		return cacheLimit;
	}

	@Override
	public void registerListener(IStoreListener listener) {
		listeners.add(listener);
	}

	@Override
	public void unregisterListener(IStoreListener listener) {
		listeners.remove(listener);
	}

	protected Region getKeyRegion(byte[] key) {
		return RegionUtil.search(regions, key);
	}

	protected Region checkKeyRegion(byte[] key) throws InvalidKeyException {
		Region region = RegionUtil.search(regions, key);
		if (region == null) {
			throw new InvalidKeyException("The given key:" + key.toString()
					+ " is not served in this data server!");
		}
		return region;
	}

	void onSet(byte[] key, byte[] value) {
		for (IStoreListener listener : listeners) {
			listener.onSet(key, value);
		}
	}

	void onGet(byte[] key, Value value) {
		for (IStoreListener listener : listeners) {
			listener.onGet(key, value);
		}
	}

	void onIncr(byte[] key, int value) {
		for (IStoreListener listener : listeners) {
			listener.onIncr(key, value);
		}
	}

	void onDelete(byte[] key) {
		for (IStoreListener listener : listeners) {
			listener.onDelete(key);
		}
	}
}
