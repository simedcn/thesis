package com.ebay.kvstore.server.data.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.kvstore.KeyValueUtil;
import com.ebay.kvstore.kvstore.PathBuilder;
import com.ebay.kvstore.kvstore.RegionUtil;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.FileLoggerInputIterator;
import com.ebay.kvstore.server.data.logger.FileRedoLogger;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.server.data.logger.IRedoLogger;
import com.ebay.kvstore.server.data.logger.SetMutation;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class MemoryStoreEngine extends BaseStoreEngine {

	private static Logger logger = LoggerFactory.getLogger(MemoryStoreEngine.class);

	protected Map<Region, IRedoLogger> loggers;

	public MemoryStoreEngine(IConfiguration conf, Region... regions) throws IOException {
		super(conf);
		loggers = new HashMap<Region, IRedoLogger>();

		long time = System.currentTimeMillis();
		for (Region r : regions) {
			String file = PathBuilder.getRegionLogPath(addr, r.getRegionId(), time);
			IRedoLogger logger = new FileRedoLogger(file);
			loggers.put(r, logger);
		}
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
	public synchronized Region unloadRegion(int regionId) {
		Region region = removeRegion(regionId);
		if (region == null) {
			return null;
		}
		IRedoLogger logger = loggers.remove(region);
		logger.close();
		cache.remove(region.getStart(), region.getEnd());
		return region;
	}

	@Override
	public synchronized void loadRegion(Address addr, Region region) {
		KeyValueCache buffer = null;
		if (regions.contains(region)) {
			return;
		}
		try {
			String regionDir = PathBuilder.getRegionDir(addr, region.getRegionId());
			String[] logFiles = RegionUtil.getRegionLogFiles(regionDir);
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
			String logFile = PathBuilder.getRegionLogPath(this.addr, region.getRegionId(),
					System.currentTimeMillis());
			IRedoLogger logger = new FileRedoLogger(logFile);
			addRegion(region);
			loggers.put(region, logger);
		} catch (IOException e) {
			logger.error("Fail to load region from" + addr + ":" + region.getRegionId(), e);
			if (buffer != null) {
				cache.removeAll(buffer);
			}
		}

	}

	@Override
	public synchronized void splitRegion(int regionId, int newRegionId) {
		Region oldRegion = getRegionById(regionId);
		if (oldRegion == null) {
			return;
		}
		List<Entry<byte[], Value>> list = new LinkedList<>();
		int size = 0;
		for (Entry<byte[], Value> e : cache) {
			if (oldRegion.compareTo(e.getKey()) == 0) {
				list.add(e);
				size += e.getKey().length + e.getValue().getSize();
			}
		}
		int current = 0;
		byte[] newKeyEnd = oldRegion.getEnd();
		byte[] oldKeyEnd = null, newKeyStart = null;
		Iterator<Entry<byte[], Value>> it = list.iterator();
		while (current < size / 2 && it.hasNext()) {
			Entry<byte[], Value> e = it.next();
			oldKeyEnd = e.getKey();
			current += e.getKey().length + e.getValue().getSize();
		}
		newKeyStart = KeyValueUtil.nextKey(oldKeyEnd);
		oldRegion.setEnd(oldKeyEnd);
		Region newRegion = new Region(newRegionId, newKeyStart, newKeyEnd, new RegionStat());
		IRedoLogger oldLogger = loggers.get(oldRegion);
		oldLogger.close();
		String logFile = oldLogger.getFile();
		long time = System.currentTimeMillis();
		String oldLogPath = PathBuilder.getRegionLogPath(addr, regionId, time);
		String newLogPath = PathBuilder.getRegionLogPath(addr, newRegionId, time);
		try {
			oldLogger = new FileRedoLogger(oldLogPath);
			IRedoLogger newLogger = new FileRedoLogger(newLogPath);
			loggers.put(oldRegion, oldLogger);
			loggers.put(newRegion, newLogger);
			addRegion(newRegion);
			FileLoggerInputIterator lit = new FileLoggerInputIterator(logFile);
			while (lit.hasNext()) {
				IMutation mutation = lit.next();
				if (oldRegion.compareTo(mutation.getKey()) == 0) {
					oldLogger.write(mutation);
				} else {
					newLogger.write(mutation);
				}
			}
		} catch (IOException e) {
			logger.error("Fail to split the loggers for regions" + e);
		}
	}

	@Override
	public long getMemoryUsed() {
		return cache.getUsed();
	}

	private IRedoLogger getRedoLogger(Region region) {
		return loggers.get(region);
	}

	@Override
	public void dispose() {
		for (Entry<Region, IRedoLogger> e : loggers.entrySet()) {
			e.getValue().close();
		}
	}

}
