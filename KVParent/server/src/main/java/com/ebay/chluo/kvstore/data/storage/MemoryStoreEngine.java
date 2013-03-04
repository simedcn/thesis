package com.ebay.chluo.kvstore.data.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public class MemoryStoreEngine extends BaseStorage {

	protected Map<Region, IRedoLogger> loggers;

	public MemoryStoreEngine(int limit) {
		super(limit);
		loggers = new HashMap<Region, IRedoLogger>();
	}

	@Override
	public void set(byte[] key, byte[] value) {
		// TODO Auto-generated method stub

	}

	@Override
	public KeyValue get(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void incr(byte[] key, int incremental, int initValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public KeyValue delete(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void registerListener(IStoreListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterListener(IStoreListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unloadRegion(int regionId) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Region> getRegions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void loadRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public void splitRegion() {
		// TODO Auto-generated method stub

	}

}
