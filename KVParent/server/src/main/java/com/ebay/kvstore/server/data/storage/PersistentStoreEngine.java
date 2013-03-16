package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.kvstore.KeyValueUtil;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.helper.IRegionLoadListener;
import com.ebay.kvstore.server.data.storage.helper.IRegionSplitListener;
import com.ebay.kvstore.server.data.storage.helper.TaskManager;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class PersistentStoreEngine extends BaseStoreEngine {

	protected Map<Region, IRegionStorage> storages;

	private static Logger logger = LoggerFactory.getLogger(PersistentStoreEngine.class);

	public PersistentStoreEngine(IConfiguration conf, Region... regions) throws IOException {
		super(conf, regions);
		storages = new HashMap<>();
		for (Region r : this.regions) {
			storages.put(r, new RegionFileStorage(conf, r, true));
		}
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
			KeyValue[] kvs = null;
			try {
				kvs = storage.getFromDisk(key);
				if (kvs != null) {
					for (KeyValue kv2 : kvs) {
						cache.set(kv2.getKey(), kv2.getValue());
						if (Arrays.equals(kv2.getKey(), key)) {
							kv = kv2;
						}
					}
				}
			} catch (IOException e) {
				logger.error("Error occured when reading from disk for key:" + key, e);
			}
		}
		return kv;
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
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
		storage.storeInBuffer(key, kv.getValue().getValue());
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
	public Region unloadRegion(int regionId) {
		Region region = removeRegion(regionId);
		if (region == null) {
			return null;
		}
		IRegionStorage storage = storages.remove(region);
		storage.dispose();
		return region;
	}

	@Override
	public void loadRegion(Address addr, Region region) {
		TaskManager.load(conf, new RegionLoadListener(), region, addr, false);
	}

	@Override
	public void splitRegion(int regionId, int newRegionId) {
		if (TaskManager.isRunning()) {
			return;
		}
		Region region = getRegionById(regionId);
		if (region == null) {
			return;
		}
		IRegionStorage storage = storages.get(region);
		TaskManager.split(storage, conf, new RegionSplitListener(storage, newRegionId));
	}

	private class RegionSplitListener implements IRegionSplitListener {

		private KeyValueCache oldBuffer;
		private int regionId;
		private IRegionStorage oldStorage;

		public RegionSplitListener(IRegionStorage storage, int regionId) {
			this.oldStorage = storage;
			this.regionId = regionId;
		}

		@Override
		public void onSplitBegin() {
			oldBuffer = oldStorage.getBuffer();
		}

		@Override
		public Region onSplitEnd(boolean success, byte[] start, byte[] end) {
			if (success) {
				return new Region(regionId, start, end, new RegionStat());
			} else {
				restore();
				return null;
			}
		}

		@Override
		public void onSplitCommit(boolean success, IRegionStorage oldStorage,
				IRegionStorage newStorage) {
			if (!success) {
				restore();
			} else {
				Region newRegion = newStorage.getRegion();
				storages.put(newRegion, newStorage);
				addRegion(newStorage.getRegion());
			}
		}

		private void restore() {
			KeyValueCache newBuffer = oldStorage.getBuffer();
			oldBuffer.addAll(newBuffer);
			oldStorage.setBuffer(oldBuffer);
		}
	}

	private class RegionLoadListener implements IRegionLoadListener {

		@Override
		public void onLoadBegin() {
			// do nothing
			logger.debug("Region load begin");
		}

		@Override
		public void onLoadEnd(boolean success) {
			logger.debug("Region load end");

		}

		@Override
		public void onLoadCommit(boolean success, IRegionStorage storage) {
			if (success) {
				storages.put(storage.getRegion(), storage);
				addRegion(storage.getRegion());
			}
			logger.debug("Region load commit");
		}

	}

	@Override
	public void dispose() {
		for (Entry<Region, IRegionStorage> s : storages.entrySet()) {
			s.getValue().closeLogger();
		}
	}
}
