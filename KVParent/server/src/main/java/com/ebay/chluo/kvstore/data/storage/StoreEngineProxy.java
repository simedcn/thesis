package com.ebay.chluo.kvstore.data.storage;

import java.util.List;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public class StoreEngineProxy implements IStoreEngine {

	protected BaseStoreEngine engine;

	public StoreEngineProxy(BaseStoreEngine engine) {
		this.engine = engine;
	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		engine.set(key, value);
		engine.onSet(key, value);
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		KeyValue kv = engine.get(key);
		engine.onGet(key, kv.getValue());
		return kv;
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		KeyValue kv = engine.incr(key, incremental, initValue);
		engine.onIncr(key, KeyValueUtil.byte2int(kv.getValue().getValue()));
		return kv;
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		engine.delete(key);
		engine.onDelete(key);
	}

	@Override
	public void flush() {
		engine.flush();
	}

	@Override
	public void unloadRegion(int regionId) {
		engine.unloadRegion(regionId);
	}

	@Override
	public void loadRegion() {
		engine.loadRegion();
	}

	@Override
	public void splitRegion() {
		engine.splitRegion();
	}

	@Override
	public void setCacheLimit(int limit) {
		engine.setCacheLimit(limit);
	}

	@Override
	public int getCacheLimit() {
		return engine.getCacheLimit();
	}

	@Override
	public void registerListener(IStoreListener listener) {
		engine.registerListener(listener);
	}

	@Override
	public void unregisterListener(IStoreListener listener) {
		engine.unregisterListener(listener);
	}

	@Override
	public List<Region> getRegions() {
		return engine.getRegions();
	}

	@Override
	public long getMemoryUsed() {
		return engine.getMemoryUsed();
	}

}
