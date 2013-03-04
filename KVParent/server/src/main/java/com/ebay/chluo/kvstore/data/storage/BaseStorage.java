package com.ebay.chluo.kvstore.data.storage;

import java.util.ArrayList;
import java.util.List;

import com.ebay.chluo.kvstore.structure.Region;

public abstract class BaseStorage implements IStoreEngine {

	protected KeyValueCache cache;

	protected int cacheLimit;

	protected List<Region> regions;

	public BaseStorage(int limit) {
		cacheLimit = limit;
		regions = new ArrayList<>();
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

}
