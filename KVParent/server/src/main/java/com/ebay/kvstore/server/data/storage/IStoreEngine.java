package com.ebay.kvstore.server.data.storage;

import java.io.IOException;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public interface IStoreEngine {

	public void addRegion(Region region, boolean create) throws IOException;

	public void delete(byte[] key) throws InvalidKeyException;

	public void dispose();

	public KeyValue get(byte[] key) throws InvalidKeyException, IOException;

	public Region[] getAllRegions();

	public int getCacheLimit();

	public long getMemoryUsed();

	public KeyValue incr(byte[] key, int incremental, int initValue, int ttl)
			throws InvalidKeyException, IOException;

	public boolean loadRegion(Region region) throws IOException;

	public void mergeRegion(int regionId1, int regionId2, int newRegionId,
			IRegionMergeCallback callback);

	public void registerListener(IStoreEngineListener listener);

	public void set(byte[] key, byte[] value, int ttl) throws InvalidKeyException;

	public void setCacheLimit(int limit);

	public void splitRegion(int regionId, int newRegionId, IRegionSplitCallback callback);

	public void stat();

	public Region unloadRegion(int regionId);

	public void unregisterListener(IStoreEngineListener listener);
}
