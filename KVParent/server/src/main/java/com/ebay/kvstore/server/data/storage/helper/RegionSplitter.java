package com.ebay.kvstore.server.data.storage.helper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.KVFileInputIterator;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class RegionSplitter extends BaseHelper {

	private static Logger logger = LoggerFactory.getLogger(RegionSplitter.class);

	protected IRegionSplitListener listener;

	public RegionSplitter(IRegionStorage storage, IConfiguration conf, IRegionSplitListener listener) {
		super(storage, conf);
		this.listener = listener;
	}

	@Override
	public void run() {
		int blockSize = conf.getInt(IConfigurationKey.Region_Block_Size);
		String dataFile = storage.getDataFile();
		String baseDir = storage.getBaseDir();
		long time = System.currentTimeMillis();
		String oldTempFile = baseDir + time;
		String newTempFile = baseDir + time + "-new";
		KeyValueCache cache = storage.getBuffer();
		KVFileInputIterator fileIt = null;
		Iterator<Entry<byte[], Value>> cacheIt = null;
		Phase phase = Phase.Begin;
		FileSystem fs = DFSManager.getDFS();
		IBlockOutputStream oldTempOut = null;
		IBlockOutputStream newTempOut = null;
		Address addr = Address.parse(conf.get(IConfigurationKey.DataServer_Addr));
		IRegionStorage newStorage = null;
		long currentSize = 0;
		byte[] oldKeyEnd = null;
		boolean splitted = false;
		try {
			listener.onSplitBegin();
			oldTempOut = new KVOutputStream(fs.create(new Path(oldTempFile)), blockSize);
			newTempOut = new KVOutputStream(fs.create(new Path(newTempFile)), blockSize);
			long fileSize = cache.getUsed();
			if (dataFile != null) {
				FileStatus status = fs.getFileStatus(new Path(dataFile));
				fileSize = fileSize + status.getLen();
				fileIt = new KVFileInputIterator(0, -1, blockSize, 0, fs.open(new Path(dataFile)));
			}
			try {
				cache.getWriteLock().lock();
				cacheIt = cache.iterator();
				KeyValue kv = null;
				Entry<byte[], Value> e = null;
				FlushResult result = flush(fileIt, cacheIt, oldTempOut, fileSize / 2, null, null);
				currentSize = result.currentSize;
				if (currentSize >= fileSize / 2) {
					splitted = true;
					oldKeyEnd = result.lastKey;
				}
				// flush the left iterators
				if (splitted) {
					if (result.kv != null) {
						result = flush(fileIt, cacheIt, newTempOut, fileSize, result.kv, result.e);
					}
					flushCache(cacheIt, newTempOut, result.e);
					flushFile(fileIt, newTempOut, result.kv);
				} else {
					// has not meet the size limit, but one of the iterator has
					// ended.
					boolean fileLeft = false;
					if (fileIt != null) {
						fileLeft = fileIt.hasNext() || result.kv != null;
					}
					if (fileLeft) {
						if (result.kv != null) {
							KeyValueUtil.writeToExternal(oldTempOut, kv);
							currentSize += KeyValueUtil.getKeyValueLen(kv);
						}
						while (fileIt.hasNext()) {
							kv = fileIt.next();
							KeyValueUtil.writeToExternal(oldTempOut, kv);
							currentSize += KeyValueUtil.getKeyValueLen(kv);
							if (currentSize >= fileSize) {
								oldKeyEnd = kv.getKey();
								break;
							}
						}
						flushFile(fileIt, newTempOut, null);
					} else {
						if (result.e != null && !result.e.getValue().isDeleted()) {
							KeyValue kv2 = new KeyValue(result.e.getKey(), result.e.getValue());
							KeyValueUtil.writeToExternal(oldTempOut, kv2);
							currentSize += KeyValueUtil.getKeyValueLen(kv2);
						}
						while (cacheIt.hasNext()) {
							e = cacheIt.next();
							kv = new KeyValue(e.getKey(), e.getValue());
							KeyValueUtil.writeToExternal(oldTempOut, kv);
							currentSize += KeyValueUtil.getKeyValueLen(kv);
							if (currentSize >= fileSize / 2) {
								oldKeyEnd = kv.getKey();
								break;
							}
						}
						flushCache(cacheIt, newTempOut, null);
					}
					storage.resetBuffer();
				}
				// TODO potential issue
				oldTempOut.close();
				newTempOut.close();

				byte[] newKeyStart = KeyValueUtil.nextKey(oldKeyEnd);
				Region oldRegion = storage.getRegion();
				// has finished the split operation.
				Region newRegion = listener.onSplitEnd(true, newKeyStart, oldRegion.getEnd());
				phase = Phase.End;
				oldRegion.setEnd(oldKeyEnd);
				String oldRegionFile = PathBuilder.getRegionFilePath(addr, oldRegion.getRegionId(),
						time);
				String newRegionFile = PathBuilder.getRegionFilePath(addr, newRegion.getRegionId(),
						time);
				String newRegionDir = PathBuilder.getRegionDir(addr, newRegion.getRegionId());
				if (!fs.exists(new Path(newRegionDir))) {
					fs.mkdirs(new Path(newRegionDir));
				}
				boolean commit = fs.rename(new Path(oldTempFile), new Path(oldRegionFile))
						&& fs.rename(new Path(newTempFile), new Path(newRegionFile));
				if (!commit) {
					listener.onSplitCommit(false, null, null);
					return;
				}
				newStorage = new RegionFileStorage(conf, newRegion);
				String oldLogFile = PathBuilder.getRegionLogPath(addr, oldRegion.getRegionId(),
						time);
				String newLogFile = PathBuilder.getRegionLogPath(addr, newRegion.getRegionId(),
						time);
				storage.newLogger(oldLogFile);
				newStorage.newLogger(newLogFile);
				storage.setDataFile(oldRegionFile);
				newStorage.setDataFile(newRegionFile);
			} finally {
				cache.getWriteLock().unlock();
			}
			listener.onSplitCommit(true, storage, newStorage);
		} catch (Exception e) {
			logger.error(
					"Error occured when splitting region:" + storage.getRegion().getRegionId(), e);
			if (phase == Phase.Begin) {
				listener.onSplitEnd(false, null, null);
			} else if (phase == Phase.End) {
				listener.onSplitCommit(false, null, null);
			}
		} finally {
			try {
				TaskManager.lock = false;
				if (newTempOut != null) {
					newTempOut.close();
				} else if (oldTempOut != null) {
					oldTempOut.close();
				}
				if (fileIt != null) {
					fileIt.close();
				}
			} catch (IOException e) {
			}

		}
	}

	private FlushResult flush(KVFileInputIterator fileIt, Iterator<Entry<byte[], Value>> cacheIt,
			IBlockOutputStream out, long max, KeyValue kv, Entry<byte[], Value> e)
			throws IOException {
		long currentSize = 0;
		KeyValue kvToWrite = null;
		byte[] lastKey = null;
		while (true) {
			if (kv == null) {
				kv = (KeyValue) nextEntry(fileIt);
			}
			if (e == null) {
				e = (Entry<byte[], Value>) nextEntry(cacheIt);
			}
			if (kv == null || e == null) {
				break;
			}
			int comp = KeyValueUtil.compare(kv.getKey(), e.getKey());
			if (comp < 0) {
				// flush the file key
				kvToWrite = kv;
				kv = null;
			} else if (comp == 0) {
				if (!e.getValue().isDeleted()) {
					kvToWrite = new KeyValue(e.getKey(), e.getValue());
				}
				kv = null;
				e = null;
			} else {
				kvToWrite = new KeyValue(e.getKey(), e.getValue());
				e = null;
			}
			if (kvToWrite != null) {
				lastKey = kvToWrite.getKey();
				KeyValueUtil.writeToExternal(out, kvToWrite);
				currentSize += KeyValueUtil.getKeyValueLen(kvToWrite);
				if (currentSize >= max) {
					break;
				}
			}
		}
		return new FlushResult(currentSize, lastKey, kv, e);
	}

	private class FlushResult {
		public long currentSize;
		public byte[] lastKey;
		public KeyValue kv;
		public Entry<byte[], Value> e;

		public FlushResult(long currentSize, byte[] lastKey, KeyValue kv, Entry<byte[], Value> e) {
			super();
			this.currentSize = currentSize;
			this.lastKey = lastKey;
			this.kv = kv;
			this.e = e;
		}
	}
}
