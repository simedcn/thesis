package com.ebay.chluo.kvstore.data.storage;

import java.util.HashMap;
import java.util.Map;

import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public class PersistentStoreRegion extends BaseStorage {

	protected Map<Region, IRegionStorage> storages;

	public PersistentStoreRegion(int limit) {
		super(limit);
		// TODO Auto-generated constructor stub
		storages = new HashMap<>();
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
	public void loadRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public void splitRegion() {
		// TODO Auto-generated method stub

	}
}
