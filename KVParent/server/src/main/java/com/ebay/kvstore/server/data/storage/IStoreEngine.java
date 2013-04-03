package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.List;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public interface IStoreEngine {

	public void delete(byte[] key) throws InvalidKeyException;

	public void dispose();

	public KeyValue get(byte[] key) throws InvalidKeyException, IOException;

	public int getCacheLimit();

	public long getMemoryUsed();

	public List<Region> getRegions();

	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException,
			IOException;

	public boolean loadRegion(Region region) throws IOException;

	public void registerListener(IStoreListener listener);

	public void set(byte[] key, byte[] value) throws InvalidKeyException;

	public void setCacheLimit(int limit);

	public void splitRegion(int regionId, int newRegionId, IRegionSplitCallback callback);

	public void stat();

	public Region unloadRegion(int regionId);

	public void unregisterListener(IStoreListener listener);
}
