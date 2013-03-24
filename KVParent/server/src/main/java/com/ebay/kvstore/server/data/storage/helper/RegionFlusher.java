package com.ebay.kvstore.server.data.storage.helper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.KVFileIterator;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

/**
 * 
 * @author luochen
 * 
 */
public class RegionFlusher extends BaseHelper {
	/**
	 * Lock for concurrent flushing
	 */
	private static Logger logger = LoggerFactory.getLogger(RegionFlusher.class);

	protected IRegionFlushListener listener;

	public RegionFlusher(IRegionStorage storage, IConfiguration conf, IRegionListener listener) {
		super(storage, conf);
		this.listener = (IRegionFlushListener) listener;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void run() {
		IBlockOutputStream out = null;
		KVFileIterator fileIt = null;
		Phase state = Phase.Begin;
		String baseDir = storage.getBaseDir();
		int blockSize = conf.getInt(IConfigurationKey.Region_Block_Size);
		String dataFile = storage.getDataFile();
		KeyValueCache buffer = storage.getBuffer();
		try {
			FileSystem fs = DFSManager.getDFS();
			if (listener != null) {
				listener.onFlushBegin();
			}
			// Just a temp file
			String target = baseDir + String.valueOf(System.currentTimeMillis());
			out = new KVOutputStream(DFSManager.getDFS().create(new Path(target), true), blockSize);
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
			}
			out.close();
			if (listener != null) {
				listener.onFlushEnd(true, target);
			}
			state = Phase.End;
			String finalFile = baseDir
					+ PathBuilder.getRegionFileName(storage.getRegion().getRegionId(),
							System.currentTimeMillis());
			boolean commitSuccess = DFSManager.getDFS().rename(new Path(target),
					new Path(finalFile));
			if (listener != null) {
				listener.onFlushCommit(commitSuccess, finalFile);
			}
			state = Phase.Commit;
		} catch (IOException e) {
			logger.error("Error Occured when flushing file", e);
			if (listener != null) {
				if (state == Phase.Begin) {
					listener.onFlushEnd(false, null);
				} else if (state == Phase.End) {
					listener.onFlushCommit(false, null);
				}
			}
		} finally {
			TaskManager.lock = false;
			try {
				if (out != null) {
					out.close();
				}
				if (fileIt != null) {
					fileIt.close();
				}
			} catch (IOException e) {
			}
		}
	}

}
