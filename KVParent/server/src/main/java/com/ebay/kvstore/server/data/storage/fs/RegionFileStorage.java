package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.RegionUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.DataFileLogger;
import com.ebay.kvstore.server.data.logger.DataFileLoggerIterator;
import com.ebay.kvstore.server.data.logger.DeleteMutation;
import com.ebay.kvstore.server.data.logger.IDataLogger;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.server.data.logger.SetMutation;
import com.ebay.kvstore.server.data.storage.task.IRegionFlushListener;
import com.ebay.kvstore.server.data.storage.task.RegionTaskManager;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

/**
 * Used for manage region data file in HDFS
 * 
 * @author luochen
 * 
 */
public class RegionFileStorage implements IRegionStorage {

	private static Logger logger = LoggerFactory.getLogger(RegionFileStorage.class);

	protected volatile Region region;

	protected volatile IDataLogger redoLogger;

	protected volatile KeyValueCache buffer;

	protected volatile KeyValueCache oldBuffer;

	protected volatile List<IndexEntry> indices;

	protected volatile String dataFile;

	protected FileSystem fs;

	protected IConfiguration conf;

	// load from IConfiguration
	protected int indexBlockNum;

	protected int blockSize;

	// TODO: Base Dir Builder
	protected volatile String baseDir;

	protected long bufferLimit;

	protected Address addr;

	// these two fields are used to record key/values in data file.
	protected volatile long dataFileSize = 0;

	protected volatile int dataFileKeyNum = 0;

	protected Lock flushingLock = new ReentrantLock();

	protected BloomFilter keyFilter;

	public RegionFileStorage(IConfiguration conf, Region region) throws IOException {
		this(conf, region, null, null, null, false);
	}

	public RegionFileStorage(IConfiguration conf, Region region, boolean logger) throws IOException {
		this(conf, region, null, null, null, true);
	}

	public RegionFileStorage(IConfiguration conf, Region region, String dataFile,
			List<IndexEntry> indices, BloomFilter filter, boolean logger) throws IOException {
		fs = DFSManager.getDFS();
		this.conf = conf;
		this.region = region;
		this.bufferLimit = conf.getInt(IConfigurationKey.Dataserver_Region_Buffer_Max);
		this.blockSize = conf.getInt(IConfigurationKey.Dataserver_Region_Block_Size);
		this.indexBlockNum = conf.getInt(IConfigurationKey.Dataserver_Region_Index_Block_Num);
		this.addr = Address.parse(conf.get(IConfigurationKey.Dataserver_Addr));
		this.buffer = KeyValueCache.forBuffer();
		if (filter != null) {
			this.keyFilter = filter;
		} else {
			this.keyFilter = new BloomFilter(conf);
		}
		if (region != null) {
			this.baseDir = PathBuilder.getRegionDir(region.getRegionId());
		}
		if (logger) {
			String file = PathBuilder.getRegionLogPath(region.getRegionId(),
					System.currentTimeMillis());
			this.redoLogger = DataFileLogger.forCreate(file);
		}
		this.dataFile = dataFile;
		if (indices == null && dataFile != null) {
			// build the index
			buildIndex();
		} else {
			this.indices = indices;
		}

	}

	public RegionFileStorage(IConfiguration conf, String dataFile) throws IOException {
		this(conf, null, dataFile, null, null, false);
	}

	@Override
	public synchronized void closeLogger() {
		if (redoLogger != null) {
			redoLogger.close();
		}
	}

	@Override
	public void deleteFromBuffer(byte[] key) {
		buffer.set(key, new Value(null, true));
		redoLogger.write(new DeleteMutation(key));
	}

	@Override
	public void dispose() {
		try {
			flushingLock.lock();
			this.closeLogger();
		} finally {
			flushingLock.unlock();
		}
	}

	private void flush() {
		logger.info("Try to flush region storage, region id:" + region.getRegionId()
				+ " current buffer size is:" + buffer.getUsed() + " buffer limit is:" + bufferLimit);
		if (!RegionTaskManager.isRunning()) {
			RegionTaskManager.flush(this, conf, new RegionFlushListener(dataFile));
		}
	}

	@Override
	public String getRegionDir() {
		if (region == null) {
			throw new UnsupportedOperationException(
					"BaseDir has not been initialized properly, as region is null");
		}
		return baseDir;
	}

	@Override
	public KeyValueCache getBuffer() {
		return buffer;
	}

	@Override
	public long getBufferLimit() {
		return bufferLimit;
	}

	@Override
	public long getBufferUsed() {
		return buffer.getUsed();
	}

	@Override
	public String getDataFile() {
		return dataFile;
	}

	@Override
	public KeyValue getFromBuffer(byte[] key) {
		KeyValue kv = buffer.get(key);
		if (kv != null) {
			return kv;
		} else {
			if (oldBuffer != null) {
				return oldBuffer.get(key);
			}
		}
		return null;
	}

	@Override
	public KeyValue[] getFromDisk(byte[] key) throws IOException {
		if (indices == null || !keyFilter.get(key)) {
			return null;
		}
		IndexEntry e = getKeyIndex(key);
		if (e == null) {
			return null;
		}
		// TODO add input file
		KVFileIterator it = new KVFileIterator(e.blockStart, e.blockEnd, blockSize, e.offset,
				fs.open(new Path(dataFile)));
		List<KeyValue> list = new LinkedList<>();
		while (it.hasNext()) {
			list.add(it.next());
		}
		KeyValue[] kvs = new KeyValue[list.size()];
		return list.toArray(kvs);
	}

	@Override
	public Region getRegion() {
		return region;
	}

	@Override
	public synchronized void newLogger(String file) throws IOException {
		if (redoLogger != null) {
			redoLogger.close();
		}
		redoLogger = DataFileLogger.forCreate(file);
	}

	@Override
	public void clear() {
		region = null;
		if (buffer != null) {
			buffer.reset();
		}
		if (indices != null) {
			indices.clear();
		}
		if (redoLogger != null) {
			redoLogger.close();
		}
	}

	@Override
	public void clearBuffer() {
		this.buffer.reset();
	}

	@Override
	public void setBuffer(KeyValueCache buffer) {
		this.buffer = buffer;
	}

	@Override
	public void setBufferLimit(int limit) {
		this.bufferLimit = limit;
	}

	@Override
	public void setDataFile(String file) throws IOException {
		String oldFile = this.dataFile;
		try {
			this.dataFile = file;
			buildIndex();
		} catch (IOException e) {
			this.dataFile = oldFile;
			throw e;
		}
	}

	@Override
	public void setLogger(String file) throws IOException {
		if (redoLogger != null) {
			redoLogger.close();
		}
		redoLogger = DataFileLogger.forAppend(file);
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	/**
	 * TODO: Just a kind of approximate. For performance issue and simplicity, I
	 * am not be able to track every get/set or traverse the data file for every
	 * time. In contrary, I record the current dataFileKeyNum and dataFileSize
	 * after every flushing, and just count the key/values in buffer. Note this
	 * may have some deviations in stat, but I think it should be acceptable.
	 */
	@Override
	public void stat() throws IOException {
		if (dataFileKeyNum == 0 && dataFile != null) {
			buildIndex();
		}
		RegionStat stat = region.getStat();
		stat.keyNum = this.dataFileKeyNum;
		stat.size = this.dataFileSize;
		try {
			buffer.getReadLock().lock();
			for (Entry<byte[], Value> e : buffer) {
				byte[] key = e.getKey();
				Value v = e.getValue();
				if (v.isDeleted()) {
					stat.keyNum--;
					stat.size -= KeyValueUtil.getKeyValueLen(key, null);
				} else {
					stat.keyNum++;
					stat.size += KeyValueUtil.getKeyValueLen(key, v);
				}
			}
		} finally {
			buffer.getReadLock().unlock();
		}
		stat.dirty = false;
	}

	@Override
	public void storeInBuffer(byte[] key, byte[] value) {
		buffer.set(key, value);
		keyFilter.set(key);
		redoLogger.write(new SetMutation(key, value));
		if (buffer.getUsed() > bufferLimit) {
			flush();
		}
	}

	/**
	 * Build index for region file
	 * 
	 * @throws IOException
	 */
	protected void buildIndex() throws IOException {
		try {
			if (indices == null) {
				indices = new ArrayList<>();
			} else {
				indices.clear();
			}
			dataFileKeyNum = IndexBuilder.build(indices, keyFilter, dataFile, blockSize,
					indexBlockNum);
			dataFileSize = FSUtil.getFileSize(dataFile);
		} catch (IOException e) {
			logger.error("Fail to build index for " + dataFile, e);
			throw e;
		}
	}

	protected void loadLogger(String file) {
		try {
			DataFileLoggerIterator it = new DataFileLoggerIterator(file);
			while (it.hasNext()) {
				IMutation mutation = it.next();
				switch (mutation.getType()) {
				case IMutation.Set:
					buffer.set(mutation.getKey(), mutation.getValue());
					break;
				case IMutation.Delete:
					buffer.set(mutation.getKey(), new Value(null, true));
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Error occured when loading the log file:" + file, e);
		}
	}

	private IndexEntry getKeyIndex(byte[] key) {
		return RegionUtil.search(indices, key);
	}

	private class RegionFlushListener implements IRegionFlushListener {
		private String oldFile;
		private List<IndexEntry> tmpIndices;
		private BloomFilter tmpFilter;
		private String oldLoggerFile;
		private String newLoggerFile;
		private int newKeyNum;

		public RegionFlushListener(String oldFile) {
			super();
			this.oldFile = oldFile;
			oldLoggerFile = redoLogger.getFile();
		}

		@Override
		public void onFlushBegin() {
			flushingLock.lock();
			logger.info("Region {} flush begin", region);
			newLoggerFile = baseDir + System.currentTimeMillis();
			try {
				newLogger(newLoggerFile);
			} catch (IOException e) {
				logger.error("Fail to create new logger", e);
			}
			oldBuffer = buffer;
			buffer = KeyValueCache.forBuffer();
		}

		@Override
		public void onFlushCommit(boolean success, String file) {
			try {
				if (success) {
					logger.info("Region {} flush success, new data file:{}", region, file);
					dataFile = file;
					indices = tmpIndices;
					keyFilter = tmpFilter;
					dataFileKeyNum = newKeyNum;
					dataFileSize = FSUtil.getFileSize(file);
					oldBuffer = null;
					long time = FSUtil.getRegionFileTimestamp(file);
					String log = PathBuilder.getRegionLogPath(region.getRegionId(), time);
					try {
						redoLogger.renameTo(log);
					} catch (IOException e) {
						logger.error("fail to create new log file" + log, e);
					}
				} else {
					logger.error("Region {} flush failed.", region);
					restore();
				}
			} finally {
				flushingLock.unlock();
			}

		}

		@Override
		public void onFlushEnd(boolean success, String file) {
			if (success) {
				try {
					tmpIndices = new ArrayList<>();
					tmpFilter = new BloomFilter(conf);
					newKeyNum = IndexBuilder.build(tmpIndices, tmpFilter, file, blockSize,
							indexBlockNum);
				} catch (Exception e) {
					logger.error("Error occured when building index for data file:" + file, e);
					restore();
					throw new RuntimeException(e);
				}
			} else {
				logger.error("Region {} flush failed", region);
				flushingLock.unlock();
				restore();
			}
		}

		private void restore() {
			oldBuffer.addAll(buffer);
			buffer = oldBuffer;
			dataFile = oldFile;
			try {
				setLogger(oldLoggerFile);
				redoLogger.append(newLoggerFile);
			} catch (IOException e) {
				logger.error("Fail to restore to old logger", e);
			}
		}
	}
}
