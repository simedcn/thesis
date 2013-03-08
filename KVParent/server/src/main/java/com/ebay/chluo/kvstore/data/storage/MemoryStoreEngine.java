package com.ebay.chluo.kvstore.data.storage;

import java.util.HashMap;
import java.util.Map;

import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.data.storage.logger.SetMutation;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public class MemoryStoreEngine extends BaseStoreEngine {

	protected Map<Region, IRedoLogger> loggers;

	public MemoryStoreEngine(int limit) {
		super(limit);
		loggers = new HashMap<Region, IRedoLogger>();
	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRedoLogger logger = getRedoLogger(region);
		cache.set(key, value);
		logger.write(new SetMutation(key, value));
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		KeyValue kv = cache.get(key);
		return kv;
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRedoLogger logger = getRedoLogger(region);
		KeyValue kv = cache.incr(key, incremental, initValue);
		logger.write(new SetMutation(key, kv.getValue().getValue()));
		return kv;
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		cache.delete(key);
	}

	@Override
	public void flush() {
		// do nothing
	}

	@Override
	public void unloadRegion(int regionId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void loadRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public void splitRegion() {
		// TODO Auto-generated method stub

	}

	private IRedoLogger getRedoLogger(Region region) {
		return loggers.get(region);
	}

	@Override
	public long getMemoryUsed() {
		return cache.getUsed();
	}

}
