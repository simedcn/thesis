package com.ebay.kvstore.server.data.storage;

import java.util.List;

import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public interface IStoreEngine {

	public void set(byte[] key, byte[] value) throws InvalidKeyException;

	public KeyValue get(byte[] key) throws InvalidKeyException;

	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException;

	public long getMemoryUsed();

	public void delete(byte[] key) throws InvalidKeyException;

	public void setCacheLimit(int limit);

	public int getCacheLimit();

	public void registerListener(IStoreListener listener);

	public void unregisterListener(IStoreListener listener);

	public Region unloadRegion(int regionId);

	public List<Region> getRegions();

	// TODO
	public void loadRegion(Address addr, Region region);

	public void splitRegion(int regionId, int newRegionId);

	public void dispose();
}
