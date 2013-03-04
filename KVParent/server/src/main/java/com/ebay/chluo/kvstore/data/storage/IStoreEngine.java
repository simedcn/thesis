package com.ebay.chluo.kvstore.data.storage;

import java.util.List;

import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public interface IStoreEngine {

	public void set(byte[] key, byte[] value);

	public KeyValue get(byte[] key);

	public void incr(byte[] key, int incremental, int initValue);

	public KeyValue delete(byte[] key);

	public void setCacheLimit(int limit);

	public int getCacheLimit();

	public void flush();

	public void registerListener(IStoreListener listener);

	public void unregisterListener(IStoreListener listener);

	public void unloadRegion(int regionId);

	public List<Region> getRegions();

	// TODO
	public void loadRegion();

	public void splitRegion();
}
