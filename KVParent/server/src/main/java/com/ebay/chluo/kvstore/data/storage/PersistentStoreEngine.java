package com.ebay.chluo.kvstore.data.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

public class PersistentStoreEngine extends BaseStoreEngine {

	protected Map<Region, IRegionStorage> storages;

	public PersistentStoreEngine(int limit) {
		super(limit);
		storages = new HashMap<>();
	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		// TODO Auto-generated method stub
		Region region = checkKeyRegion(key);
		IRegionStorage storage = storages.get(region);
		storage.storeInBuffer(key, value);
		cache.set(key, value);
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRegionStorage storage = storages.get(region);
		// 1. get from buffer
		KeyValue kv = storage.getFromBuffer(key);
		if (kv != null) {
			if (kv.getValue().isDeleted()) {
				return null;
			} else {
				return kv;
			}
		} else if ((kv = cache.get(key)) != null) {
			// 2.get from cache
			return kv;
		} else {
			// 3. get from disk
			KeyValue[] kvs = storage.getFromDisk(key);
			for (KeyValue kv2 : kvs) {
				cache.set(kv.getKey(), kv.getValue());
				if (kv2.equals(kv)) {
					kv = kv2;
				}
			}
		}
		return kv;
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		// TODO Auto-generated method stub
		KeyValue kv = this.get(key);
		byte[] value = null;
		if (kv == null) {
			value = KeyValueUtil.intToBytes(initValue + incremental);
			kv = new KeyValue(key, new Value(value));
		} else {
			kv.getValue().incr(incremental);
		}
		Region region = getKeyRegion(key);
		IRegionStorage storage = storages.get(region);
		storage.storeInBuffer(key, value);
		cache.set(key, value);

		return kv;
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRegionStorage storage = storages.get(region);
		storage.deleteFromBuffer(key);
		cache.delete(key);
	}

	@Override
	public long getMemoryUsed() {
		long result = cache.getUsed();
		Set<Entry<Region, IRegionStorage>> entries = storages.entrySet();
		for (Entry<Region, IRegionStorage> e : entries) {
			result += e.getValue().getBufferUsed();
		}
		return result;
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
