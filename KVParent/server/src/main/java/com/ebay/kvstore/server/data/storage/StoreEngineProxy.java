package com.ebay.kvstore.server.data.storage;

import java.io.IOException;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.util.KeyValueUtil;

public class StoreEngineProxy implements IStoreEngine {

	protected BaseStoreEngine engine;

	public StoreEngineProxy(BaseStoreEngine engine) {
		this.engine = engine;
	}

	@Override
	public void addRegion(Region region, boolean create) throws IOException {
		engine.addRegion(region, create);
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		engine.delete(key);
		engine.onDelete(key);
	}

	@Override
	public void dispose() {
		engine.dispose();
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException, IOException {
		KeyValue kv = engine.get(key);
		if (kv != null) {
			engine.onGet(key, kv.getValue());
		}
		return kv;
	}

	@Override
	public Region[] getAllRegions() {
		return engine.getAllRegions();
	}

	@Override
	public int getCacheLimit() {
		return engine.getCacheLimit();
	}

	@Override
	public long getMemoryUsed() {
		return engine.getMemoryUsed();
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue, int ttl)
			throws InvalidKeyException, IOException {
		KeyValue kv = engine.incr(key, incremental, initValue, ttl);
		engine.onIncr(key, KeyValueUtil.bytesToInt(kv.getValue().getValue()));
		return kv;
	}

	@Override
	public boolean loadRegion(Region region) throws IOException {
		return engine.loadRegion(region);
	}

	@Override
	public void mergeRegion(int regionId1, int regionId2, int newRegionId,
			IRegionMergeCallback callback) {
		engine.mergeRegion(regionId1, regionId2, newRegionId, callback);
	}

	@Override
	public void registerListener(IStoreEngineListener listener) {
		engine.registerListener(listener);
	}

	@Override
	public void set(byte[] key, byte[] value, int ttl) throws InvalidKeyException {
		engine.set(key, value, ttl);
		engine.onSet(key, value);
	}

	@Override
	public void setCacheLimit(int limit) {
		engine.setCacheLimit(limit);
	}

	@Override
	public void splitRegion(int regionId, int newRegionId, IRegionSplitCallback callback) {
		engine.splitRegion(regionId, newRegionId, callback);
	}

	@Override
	public void stat() {
		engine.stat();
	}

	@Override
	public Region unloadRegion(int regionId) {
		return engine.unloadRegion(regionId);
	}

	@Override
	public void unregisterListener(IStoreEngineListener listener) {
		engine.unregisterListener(listener);
	}

}
