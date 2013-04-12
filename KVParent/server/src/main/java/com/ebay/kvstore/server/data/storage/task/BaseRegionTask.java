package com.ebay.kvstore.server.data.storage.task;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.KVFileIterator;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public abstract class BaseRegionTask implements Runnable {
	protected IRegionStorage storage;
	protected IConfiguration conf;
	protected FileSystem fs;
	protected int blockSize;

	public BaseRegionTask(IRegionStorage storage, IConfiguration conf) {
		super();
		this.storage = storage;
		this.conf = conf;
		this.fs = DFSManager.getDFS();
		this.blockSize = conf.getInt(IConfigurationKey.Dataserver_Region_Block_Size);
	}

	protected void flushCache(Iterator<Entry<byte[], Value>> it, IBlockOutputStream out,
			Entry<byte[], Value> e) throws IOException {
		if (e != null && !e.getValue().isDeleted()) {
			KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
		}
		while (it.hasNext()) {
			e = it.next();
			if (!e.getValue().isDeleted()) {
				KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
			}
		}
	}

	protected void flushFile(Iterator<KeyValue> it, IBlockOutputStream out, KeyValue kv)
			throws IOException {
		if (kv != null) {
			KeyValueUtil.writeToExternal(out, kv);
		}
		while (it.hasNext()) {
			kv = it.next();
			KeyValueUtil.writeToExternal(out, kv);
		}
	}

	@SuppressWarnings("unchecked")
	protected void flushRegion(IBlockOutputStream out, KeyValueCache buffer, String dataFile)
			throws IOException {
		KVFileIterator fileIt = null;
		try {
			buffer.getReadLock().lock();
			Iterator<Entry<byte[], Value>> cacheIt = buffer.iterator();
			if (dataFile == null) {
				flushCache(cacheIt, out, null);
			} else {
				fileIt = new KVFileIterator(0, -1, blockSize, 0, fs.open(new Path(dataFile)));
				KeyValue kv = null; // from File
				Entry<byte[], Value> e = null; // from Cache
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
						KeyValueUtil.writeToExternal(out, kv);
						kv = null;
					} else if (comp == 0) {
						if (!e.getValue().isDeleted()) {
							KeyValueUtil.writeToExternal(out,
									new KeyValue(e.getKey(), e.getValue()));
						}
						kv = null;
						e = null;
					} else {
						if (!e.getValue().isDeleted()) {
							KeyValueUtil.writeToExternal(out,
									new KeyValue(e.getKey(), e.getValue()));
						}
						e = null;
					}
				}
				flushCache(cacheIt, out, null);
				flushFile(fileIt, out, null);
			}
		} finally {
			buffer.getReadLock().unlock();
			if (fileIt != null) {
				fileIt.close();
			}
		}
	}

	@SuppressWarnings("rawtypes")
	protected Object nextEntry(Iterator it) {
		if (it == null) {
			return null;
		}
		if (it.hasNext()) {
			return it.next();
		} else {
			return null;
		}
	}
}
