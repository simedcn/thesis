package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.RegionUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.FileDataLogger;
import com.ebay.kvstore.server.data.logger.FileDataLoggerIterator;
import com.ebay.kvstore.server.data.logger.IDataLogger;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.server.data.logger.SetMutation;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class MemoryStoreEngine extends BaseStoreEngine {

	private static Logger logger = LoggerFactory.getLogger(MemoryStoreEngine.class);

	protected Map<Region, IDataLogger> loggers;

	public MemoryStoreEngine(IConfiguration conf, Region... regions) throws IOException {
		super(conf);
		loggers = new HashMap<Region, IDataLogger>();

		long time = System.currentTimeMillis();
		for (Region r : regions) {
			String file = PathBuilder.getRegionLogPath(r.getRegionId(), time);
			IDataLogger logger = FileDataLogger.forCreate(file);
			loggers.put(r, logger);
		}
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		cache.delete(key);
	}

	@Override
	public void dispose() {
		for (Entry<Region, IDataLogger> e : loggers.entrySet()) {
			e.getValue().close();
		}
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		KeyValue kv = cache.get(key);
		return kv;
	}

	@Override
	public long getMemoryUsed() {
		return cache.getUsed();
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IDataLogger logger = getRedoLogger(region);
		KeyValue kv = cache.incr(key, incremental, initValue);
		logger.write(new SetMutation(key, kv.getValue().getValue()));
		return kv;
	}

	@Override
	public synchronized void loadRegion(Region region) throws IOException {
		KeyValueCache buffer = null;
		if (regions.contains(region)) {
			return;
		}
		try {
			String regionDir = PathBuilder.getRegionDir(region.getRegionId());
			String[] logFiles = FSUtil.getRegionLogFiles(regionDir);
			boolean success = false;
			for (int i = logFiles.length - 1; i >= 0; i--) {
				buffer = KeyValueCache.forBuffer();
				try {
					RegionUtil.loadLogger(regionDir + logFiles[i], buffer);
					success = true;
					break;
				} catch (Exception e) {
					logger.error("Fail to load region from " + logFiles[i] + ", try to load next",
							e);
				}
			}
			if (!success) {
				logger.error("Fail to load region from all available log files");
				return;
			}
			// there should be no overlap in keys
			cache.addAll(buffer);
			String logFile = PathBuilder.getRegionLogPath(region.getRegionId(),
					System.currentTimeMillis());
			IDataLogger logger = FileDataLogger.forCreate(logFile);
			addRegion(region);
			loggers.put(region, logger);
		} catch (IOException e) {
			logger.error("Fail to load region from" + addr + ":" + region.getRegionId(), e);
			if (buffer != null) {
				cache.removeAll(buffer);
			}
			throw e;
		}

	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IDataLogger logger = getRedoLogger(region);
		cache.set(key, value);
		logger.write(new SetMutation(key, value));
	}

	@Override
	public synchronized void splitRegion(int regionId, int newRegionId,
			IRegionSplitCallback callback) {
		boolean finished = false;
		Region newRegion = null;
		Region oldRegion = null;
		try {
			oldRegion = getRegionById(regionId);
			if (oldRegion == null) {
				return;
			}
			List<Entry<byte[], Value>> list = new LinkedList<>();
			int size = 0;
			// traverse the cache
			try {
				cache.getReadLock().lock();
				for (Entry<byte[], Value> e : cache) {
					if (oldRegion.compareTo(e.getKey()) == 0) {
						list.add(e);
						size += KeyValueUtil.getKeyValueLen(e.getKey(), e.getValue());
					}
				}
			} finally {
				cache.getReadLock().unlock();
			}
			int current = 0;
			byte[] newKeyEnd = oldRegion.getEnd();
			byte[] oldKeyEnd = null, newKeyStart = null;
			Iterator<Entry<byte[], Value>> it = list.iterator();
			while (current < size / 2 && it.hasNext()) {
				Entry<byte[], Value> e = it.next();
				oldKeyEnd = e.getKey();
				current += KeyValueUtil.getKeyValueLen(e.getKey(), e.getValue());
			}
			newKeyStart = KeyValueUtil.nextKey(oldKeyEnd);
			oldRegion.setEnd(oldKeyEnd);
			newRegion = new Region(newRegionId, newKeyStart, newKeyEnd);
			IDataLogger oldLogger = loggers.get(oldRegion);
			oldLogger.close();
			String logFile = oldLogger.getFile();
			long time = System.currentTimeMillis();
			String oldLogPath = PathBuilder.getRegionLogPath(regionId, time);
			String newLogPath = PathBuilder.getRegionLogPath(newRegionId, time);
			try {
				oldLogger = FileDataLogger.forCreate(oldLogPath);
				IDataLogger newLogger = FileDataLogger.forCreate(newLogPath);
				loggers.put(oldRegion, oldLogger);
				loggers.put(newRegion, newLogger);
				addRegion(newRegion);
				FileDataLoggerIterator lit = new FileDataLoggerIterator(logFile);
				while (lit.hasNext()) {
					IMutation mutation = lit.next();
					if (oldRegion.compareTo(mutation.getKey()) == 0) {
						oldLogger.write(mutation);
					} else {
						newLogger.write(mutation);
					}
				}
				finished = true;
			} catch (IOException e) {
				logger.error("Fail to split the loggers for regions" + e);
			}

		} finally {
			callback.callback(finished, oldRegion, newRegion);
		}

	}

	@Override
	public synchronized void stat() {
		boolean dirty = false;
		List<Region> dirtyList = new ArrayList<>();
		for (Region region : regions) {
			if (region.isDirty()) {
				region.getStat().reset();
				dirtyList.add(region);
				dirty = true;
			}
		}
		if (!dirty) {
			return;
		}
		try {
			cache.getReadLock().lock();
			Iterator<Entry<byte[], Value>> it = cache.iterator();
			Region region = null;
			Entry<byte[], Value> e = null;
			while (it.hasNext()) {
				e = it.next();
				region = getKeyRegion(dirtyList, e.getKey());
				if (region != null) {
					region.getStat().keyNum++;
					region.getStat().size += KeyValueUtil.getKeyValueLen(e.getKey(), e.getValue());
				}
			}
			for (Region r : dirtyList) {
				r.getStat().dirty = false;
			}
		} finally {
			cache.getReadLock().unlock();
		}

	}

	@Override
	public synchronized Region unloadRegion(int regionId) {
		Region region = removeRegion(regionId);
		if (region == null) {
			return null;
		}
		IDataLogger logger = loggers.remove(region);
		logger.close();
		cache.remove(region.getStart(), region.getEnd());
		return region;
	}

	private IDataLogger getRedoLogger(Region region) {
		return loggers.get(region);
	}
}
