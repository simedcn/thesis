package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.List;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public class StoreEngineProxy implements IStoreEngine {

	protected BaseStoreEngine engine;

	public StoreEngineProxy(BaseStoreEngine engine) {
		this.engine = engine;
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
	public int getCacheLimit() {
		return engine.getCacheLimit();
	}

	@Override
	public long getMemoryUsed() {
		return engine.getMemoryUsed();
	}

	@Override
	public List<Region> getRegions() {
		return engine.getRegions();
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException,
			IOException {
		KeyValue kv = engine.incr(key, incremental, initValue);
		engine.onIncr(key, KeyValueUtil.bytesToInt(kv.getValue().getValue()));
		return kv;
	}

	@Override
	public void loadRegion(Region region) throws IOException {
		engine.loadRegion(region);
	}

	@Override
	public void registerListener(IStoreListener listener) {
		engine.registerListener(listener);
	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		engine.set(key, value);
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
	public void unregisterListener(IStoreListener listener) {
		engine.unregisterListener(listener);
	}

}
