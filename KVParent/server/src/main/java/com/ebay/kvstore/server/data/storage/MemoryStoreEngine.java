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
import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.DataFileLogger;
import com.ebay.kvstore.server.data.logger.DataFileLoggerIterator;
import com.ebay.kvstore.server.data.logger.DeleteMutation;
import com.ebay.kvstore.server.data.logger.IDataLogger;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.server.data.logger.SetMutation;
import com.ebay.kvstore.server.monitor.IMonitorObject;
import com.ebay.kvstore.server.monitor.IPerformanceMonitor;
import com.ebay.kvstore.server.monitor.MonitorFactory;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class MemoryStoreEngine extends BaseStoreEngine {

	private static Logger logger = LoggerFactory.getLogger(MemoryStoreEngine.class);

	protected Map<Region, IDataLogger> loggers;

	private IPerformanceMonitor monitor;

	public MemoryStoreEngine(IConfiguration conf, Region... regions) throws IOException {
		super(conf);
		loggers = new HashMap<Region, IDataLogger>();
		for (Region r : regions) {
			addRegion(r, true);
		}
		monitor = MonitorFactory.getMonitor();
	}

	@Override
	public void addRegion(Region region, boolean create) throws IOException {
		if (create) {
			if (regions.contains(region)) {
				return;
			}
			long time = System.currentTimeMillis();
			String file = PathBuilder.getRegionLogPath(region.getRegionId(), time);
			IDataLogger logger = DataFileLogger.forCreate(file);
			addRegion(region);
			loggers.put(region, logger);
		} else {
			loadRegion(region);
		}
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Delete_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			cache.delete(key);
			IDataLogger logger = getRedoLogger(region);
			logger.write(new DeleteMutation(key));
		} finally {
			object.stop();
		}
	}

	@Override
	public synchronized void dispose() {
		for (Entry<Region, IDataLogger> e : loggers.entrySet()) {
			e.getValue().close();
		}
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Get_Monitor);
		try {
			object.start();
			checkKeyRegion(key);
			KeyValue kv = cache.get(key);
			return kv;
		} finally {
			object.stop();
		}
	}

	@Override
	public long getMemoryUsed() {
		return cache.getUsed();
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Incr_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			IDataLogger logger = getRedoLogger(region);
			KeyValue kv = cache.incr(key, incremental, initValue);
			logger.write(new SetMutation(key, kv.getValue().getValue()));
			return kv;
		} finally {
			object.stop();
		}

	}

	@Override
	public synchronized boolean loadRegion(Region region) throws IOException {
		KeyValueCache buffer = null;
		if (regions.contains(region)) {
			return true;
		}
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Load_Monitor);
		try {
			object.start();
			String regionDir = PathBuilder.getRegionDir(region.getRegionId());
			String[] logFiles = FSUtil.getRegionLogFiles(regionDir);
			boolean success = false;
			if (logFiles == null || logFiles.length == 0) {
				success = true;
			} else {
				for (int i = logFiles.length - 1; i >= 0; i--) {
					buffer = KeyValueCache.forBuffer();
					try {
						RegionUtil.loadLogger(regionDir + logFiles[i], buffer);
						success = true;
						break;
					} catch (Exception e) {
						logger.error("Fail to load region from " + logFiles[i]
								+ ", try to load next", e);
					}
				}
			}
			if (!success) {
				logger.error("Fail to load region from all available log files");
				return false;
			}
			// there should be no overlap in keys
			cache.addAll(buffer);
			String logFile = PathBuilder.getRegionLogPath(region.getRegionId(),
					System.currentTimeMillis());
			IDataLogger logger = DataFileLogger.forCreate(logFile);
			addRegion(region);
			loggers.put(region, logger);
			return true;
		} catch (IOException e) {
			logger.error("Fail to load region from" + addr + ":" + region.getRegionId(), e);
			if (buffer != null) {
				cache.removeAll(buffer);
			}
			throw e;
		} finally {
			object.stop();
		}

	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Set_Monitor);
		try {
			object.start();
			Region region = checkKeyRegion(key);
			IDataLogger logger = getRedoLogger(region);
			cache.set(key, value);
			logger.write(new SetMutation(key, value));
		} finally {
			object.stop();
		}

	}

	@Override
	public void splitRegion(int regionId, int newRegionId, IRegionSplitCallback callback) {
		boolean finished = false;
		Region newRegion = null;
		Region oldRegion = null;
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Split_Monitor);
		try {
			object.start();
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
				oldLogger = DataFileLogger.forCreate(oldLogPath);
				IDataLogger newLogger = DataFileLogger.forCreate(newLogPath);
				loggers.put(oldRegion, oldLogger);
				loggers.put(newRegion, newLogger);
				addRegion(newRegion);
				DataFileLoggerIterator lit = new DataFileLoggerIterator(logFile);
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
			if (callback != null) {
				callback.callback(finished, oldRegion, newRegion);
			}
			object.stop();
		}
	}

	@Override
	public void mergeRegion(int regionId1, int regionId2, int newRegionId,
			IRegionMergeCallback callback) {
		Region region1 = getRegionById(regionId1);
		Region region2 = getRegionById(regionId2);
		Region region = null;
		boolean finished = false;
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Merge_Monitor);
		try {
			object.start();
			if (region1 == null || region2 == null || !checkRegionConsistent(region1, region2)) {
				return;
			}
			byte[] start = null;
			byte[] end = null;
			if (region1.compareTo(region2) < 0) {
				start = region1.getStart();
				end = region2.getEnd();
			} else {
				start = region2.getStart();
				end = region1.getEnd();
			}
			RegionStat stat1 = region1.getStat();
			RegionStat stat2 = region2.getStat();
			RegionStat stat = (RegionStat) stat1.clone();
			stat.keyNum += stat2.keyNum;
			stat.readCount += stat2.readCount;
			stat.writeCount += stat2.writeCount;
			stat.size += stat2.size;
			region = new Region(newRegionId, start, end);
			region.setStat(stat);

			IDataLogger logger1 = loggers.remove(region1);
			IDataLogger logger2 = loggers.remove(region2);
			logger1.close();
			logger2.close();
			long time = System.currentTimeMillis();
			String loggerPath = PathBuilder.getRegionLogPath(newRegionId, time);
			IDataLogger logger = DataFileLogger.forCreate(loggerPath);
			logger.append(logger1.getFile());
			logger.append(logger2.getFile());
			addRegion(region);
			loggers.put(region, logger);
			finished = true;
		} catch (IOException e) {
			logger.error("Error occured when merge region " + regionId1 + " and " + regionId2, e);
		} finally {
			if (callback != null) {
				callback.callback(finished, regionId1, regionId2, region);
			}
			object.stop();
		}

	}

	@Override
	public void stat() {
		IMonitorObject object = monitor.getMonitorObject(IPerformanceMonitor.Memory_Stat_Monitor);
		try {
			object.start();
			cache.getReadLock().lock();
			List<Region> dirtyList = new ArrayList<>();
			for (Region region : regions) {
				if (region.isDirty()) {
					region.getStat().reset();
					dirtyList.add(region);
				}
			}
			if (dirtyList.size() == 0) {
				return;
			}
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
			object.stop();
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
