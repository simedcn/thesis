package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.RegionUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.task.IRegionLoadListener;
import com.ebay.kvstore.server.data.storage.task.IRegionMergeListener;
import com.ebay.kvstore.server.data.storage.task.IRegionSplitListener;
import com.ebay.kvstore.server.data.storage.task.RegionTaskManager;
import com.ebay.kvstore.server.monitor.IMonitorObject;
import com.ebay.kvstore.server.monitor.IPerformanceMonitor;
import com.ebay.kvstore.server.monitor.MonitorFactory;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class PersistentStoreEngine extends BaseStoreEngine {

	protected Map<Region, IRegionStorage> storages;

	private static Logger logger = LoggerFactory.getLogger(PersistentStoreEngine.class);

	private IPerformanceMonitor monitor;

	public PersistentStoreEngine(IConfiguration conf, Region... regions) throws IOException {
		super(conf, regions);
		storages = new HashMap<>();
		for (Region r : this.regions) {
			storages.put(r, new RegionFileStorage(conf, r, true));
		}
		monitor = MonitorFactory.getMonitor();
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		IMonitorObject object = monitor
				.getMonitorObject(IPerformanceMonitor.Persistent_Delete_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			IRegionStorage storage = storages.get(region);
			storage.deleteFromBuffer(key);
			cache.delete(key);

		} finally {
			object.stop();
		}

	}

	@Override
	public synchronized void dispose() {
		for (Entry<Region, IRegionStorage> s : storages.entrySet()) {
			try {
				s.getValue().dispose();
			} catch (Exception e) {
				logger.error("Error occured when stop region storage", e);
			}
		}
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException, IOException {
		IMonitorObject object = monitor
				.getMonitorObject(IPerformanceMonitor.Persistent_Get_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			IRegionStorage storage = storages.get(region);
			// 1. get from buffer
			KeyValue kv = storage.getFromBuffer(key);
			if (kv != null) {
				if (kv.getValue().isDeleted() || !KeyValueUtil.isAlive(kv.getValue())) {
					return null;
				} else {
					return kv;
				}
			} else if ((kv = cache.get(key)) != null) {
				// 2.get from cache
				if (KeyValueUtil.isAlive(kv.getValue())) {
					return kv;
				} else {
					return null;
				}
			} else {
				// 3. get from disk
				KeyValue[] kvs = null;
				try {
					kvs = storage.getFromDisk(key);
					if (kvs != null) {
						for (KeyValue kv2 : kvs) {
							if (KeyValueUtil.isAlive(kv2.getValue())) {
								if (Arrays.equals(kv2.getKey(), key)) {
									kv = kv2;
								}
								cache.set(kv2.getKey(), kv2.getValue());
							}
						}
					}
				} catch (IOException e) {
					logger.error("Error occured when reading from disk for key:" + key, e);
					throw e;
				}
			}
			return kv;
		} finally {
			object.stop();
		}
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
	public KeyValue incr(byte[] key, int incremental, int initValue, int ttl)
			throws InvalidKeyException, IOException {
		IMonitorObject object = monitor
				.getMonitorObject(IPerformanceMonitor.Persistent_Incr_Monitor);
		try {
			object.start();
			KeyValue kv = this.get(key);
			kv = incr(kv, key, initValue, incremental, ttl);
			Region region = getKeyRegion(key);
			IRegionStorage storage = storages.get(region);
			storage.storeInBuffer(key, kv.getValue());
			cache.set(key, kv.getValue());
			return kv;
		} finally {
			object.stop();
		}

	}

	@Override
	public boolean loadRegion(Region region) {
		if (!regions.contains(region)) {
			return RegionTaskManager.load(conf, new RegionLoadListener(), region);
		}
		return true;
	}

	@Override
	public void set(byte[] key, byte[] value, int ttl) throws InvalidKeyException {
		IMonitorObject object = monitor
				.getMonitorObject(IPerformanceMonitor.Persistent_Set_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			IRegionStorage storage = storages.get(region);
			long expire = KeyValueUtil.getExpireTime(ttl);
			Value v = new Value(value, expire);
			storage.storeInBuffer(key, v);
			cache.set(key, v);
		} finally {
			object.stop();
		}

	}

	@Override
	public void splitRegion(int regionId, int newRegionId, IRegionSplitCallback callback) {
		logger.info("Try to split region " + regionId + " into new region " + newRegionId);
		if (RegionTaskManager.isRunning()) {
			return;
		}
		Region region = getRegionById(regionId);
		if (region == null) {
			if (callback != null) {
				callback.callback(false, null, null);
			}
			return;
		}
		IRegionStorage storage = storages.get(region);
		RegionTaskManager.split(storage, conf, new RegionSplitListener(storage, newRegionId,
				callback));
	}

	public void mergeRegion(int regionId1, int regionId2, int newRegionId,
			IRegionMergeCallback callback) {
		Region region1 = getRegionById(regionId1);
		Region region2 = getRegionById(regionId2);
		if (region1 == null || region2 == null || !checkRegionConsistent(region1, region2)) {
			if (callback != null) {
				callback.callback(false, regionId1, regionId2, null);
			}
			return;
		}
		IRegionStorage storage1 = storages.get(region1);
		IRegionStorage storage2 = storages.get(region2);
		RegionTaskManager.merge(conf, storage1, storage2, new RegionMergeListener(callback,
				regionId1, regionId2, newRegionId));

	}

	@Override
	public void stat() {
		IMonitorObject object = monitor
				.getMonitorObject(IPerformanceMonitor.Persistent_Stat_Monitor);
		try {
			object.start();
			for (Entry<Region, IRegionStorage> e : storages.entrySet()) {
				if (e.getKey().getStat().dirty) {
					try {
						e.getValue().stat();
					} catch (Exception ex) {
						logger.error("Error occured when stat the region:" + e.getValue(), ex);
					}
				}
			}
		} finally {
			object.stop();
		}

	}

	@Override
	public Region unloadRegion(int regionId) {
		return removeRegion(regionId);
	}

	protected Region removeRegion(int regionId) {
		Region region = super.removeRegion(regionId);
		if (region == null) {
			return null;
		}
		IRegionStorage storage = storages.remove(region);
		storage.dispose();
		return region;
	}

	protected void addRegion(Region region, IRegionStorage storage) {
		storages.put(region, storage);
		addRegion(region);
	}

	@Override
	public void addRegion(Region region, boolean create) throws IOException {
		if (create) {
			if (regions.contains(region)) {
				return;
			}
			IRegionStorage storage = new RegionFileStorage(conf, region, true);
			addRegion(region);
			storages.put(region, storage);
		} else {
			loadRegion(region);
		}

	}

	private class RegionLoadListener implements IRegionLoadListener {
		private IMonitorObject object;

		@Override
		public void onLoadBegin() {
			object = monitor.getMonitorObject(IPerformanceMonitor.Persistent_Load_Monitor);
			object.start();
			logger.info("Region load begin");
		}

		@Override
		public void onLoadCommit(boolean success, IRegionStorage storage) {
			if (success) {
				logger.info("Region load success");
				onLoad(storage.getRegion());
				storages.put(storage.getRegion(), storage);
				addRegion(storage.getRegion());
			} else {
				logger.info("Region load commit failed");
			}
			object.stop();
		}

		@Override
		public void onLoadEnd(boolean success) {
			logger.info("Region load end, result:{}", success ? "success" : "fail");
			if (!success) {
				object.stop();
			}
		}
	}

	private class RegionSplitListener implements IRegionSplitListener {

		private KeyValueCache oldBuffer;
		private int regionId;
		private IRegionStorage oldStorage;
		private IRegionSplitCallback callback;
		private IMonitorObject object;

		public RegionSplitListener(IRegionStorage storage, int regionId,
				IRegionSplitCallback callback) {
			this.oldStorage = storage;
			this.regionId = regionId;
			this.callback = callback;
		}

		@Override
		public void onSplitBegin() {
			object = monitor.getMonitorObject(IPerformanceMonitor.Persistent_Split_Monitor);
			object.start();
			oldBuffer = oldStorage.getBuffer();
			logger.info("Region split begin, try to split region " + oldStorage.getRegion());
		}

		@Override
		public void onSplitCommit(boolean success, IRegionStorage oldStorage,
				IRegionStorage newStorage) {
			if (!success) {
				restore();
				if (callback != null) {
					callback.callback(false, this.oldStorage.getRegion(), null);
				}
			} else {
				Region newRegion = newStorage.getRegion();
				addRegion(newRegion, newStorage);
				onSplit(oldStorage.getRegion(), newRegion);
				if (callback != null) {
					callback.callback(true, this.oldStorage.getRegion(), newRegion);
				}
				object.stop();
				logger.info("Region split success , region has been splitted to {} and {}",
						oldStorage.getRegion(), newStorage.getRegion());
			}
		}

		@Override
		public Region onSplitEnd(boolean success, byte[] start, byte[] end) {
			if (success) {
				return new Region(regionId, start, end);
			} else {
				restore();
				if (callback != null) {
					callback.callback(false, this.oldStorage.getRegion(), null);
				}
				return null;
			}
		}

		private void restore() {
			logger.info("Region split failed, source region is:" + oldStorage.getRegion());
			KeyValueCache newBuffer = oldStorage.getBuffer();
			oldBuffer.addAll(newBuffer);
			oldStorage.setBuffer(oldBuffer);
			object.stop();
		}
	}

	private class RegionMergeListener implements IRegionMergeListener {

		private IRegionMergeCallback callback;

		private int newRegionId;

		private int region1;
		private int region2;
		private IMonitorObject object;

		public RegionMergeListener(IRegionMergeCallback callback, int region1, int region2,
				int newRegionId) {
			super();
			this.callback = callback;
			this.region1 = region1;
			this.region2 = region2;
			this.newRegionId = newRegionId;
		}

		@Override
		public void onMergeBegin() {
			object = monitor.getMonitorObject(IPerformanceMonitor.Persistent_Merge_Monitor);
			object.start();
			logger.info("Region merge start, try to merge region {} and {}", region1, region2);
		}

		@Override
		public void onMergeCommit(boolean success, IRegionStorage storage) {
			if (success) {
				removeRegion(region1);
				removeRegion(region2);
				addRegion(storage.getRegion(), storage);
				if (callback != null) {
					callback.callback(success, region1, region2, storage.getRegion());
				}
				logger.info("Region merge success, new region is {}", storage.getRegion());
			} else {
				if (callback != null) {
					callback.callback(false, region1, region2, null);
				}
				logger.info("Region merge failed");
			}
			object.stop();
		}

		@Override
		public Region onMergeEnd(boolean success, Region region1, Region region2) {
			Region region = null;
			if (success) {
				region = RegionUtil.mergeRegion(region1, region2, newRegionId);
			} else {
				if (callback != null) {
					callback.callback(false, this.region1, this.region2, null);
				}
				logger.info("Region merge failed");
				object.stop();
			}
			return region;
		}

	}
}
