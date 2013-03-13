package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.RegionUtil;
import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.data.storage.logger.DeleteMutation;
import com.ebay.chluo.kvstore.data.storage.logger.IMutation;
import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.data.storage.logger.LoggerFSInputIterator;
import com.ebay.chluo.kvstore.data.storage.logger.SetMutation;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

/**
 * Used for manage region data file in HDFS
 * 
 * @author luochen
 * 
 */
public class FSRegionStorage implements IRegionStorage {

	private static Logger logger = LoggerFactory.getLogger(FSRegionStorage.class);

	protected Region region;

	protected IRedoLogger redoLogger;

	protected KeyValueCache buffer;

	protected long bufferLimit;

	protected List<IndexEntry> indices;

	protected int blockSize;

	// TODO: Base Dir Builder
	protected String baseDir;

	protected String dataFile;

	protected int indexBlockCount;

	protected DFSClient fsclient;

	public FSRegionStorage(int bufferLimit) {
		this.bufferLimit = bufferLimit;
		// The KeyValue cache will not replace entries automatically, in
		// contrast, we will check the buffer size manually.
		this.buffer = new KeyValueCache(0, null);
	}

	public FSRegionStorage(int bufferLimit, Region region) {
		this.bufferLimit = bufferLimit;
		this.region = region;
		// The KeyValue cache will not replace entries automatically, in
		// contrast, we will check the buffer size manually.
		this.buffer = new KeyValueCache(0, null);
	}

	@Override
	public void load() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBufferLimit(int limit) {
		this.bufferLimit = limit;
	}

	@Override
	public long getBufferLimit() {
		return bufferLimit;
	}

	@Override
	public KeyValue getFromBuffer(byte[] key) {
		return buffer.get(key);
	}

	@Override
	public KeyValue[] getFromDisk(byte[] key) throws IOException {
		IndexEntry e = getKeyIndex(key);
		if (e == null) {
			return null;
		}
		// TODO add input file
		KVFSInputIterator it = new KVFSInputIterator(e.blockStart, e.blockEnd, e.offset, blockSize,
				null);
		List<KeyValue> list = new LinkedList<>();
		while (it.hasNext()) {
			list.add(it.next());
		}
		KeyValue[] kvs = new KeyValue[list.size()];
		return list.toArray(kvs);
	}

	@Override
	public void commit() {
		if (!KeyValueFlusher.isRunning()) {
			KeyValueFlusher.flush(buffer, baseDir, dataFile, region, blockSize,
					new RegionFlushListener(buffer, dataFile));
		}
	}

	@Override
	public void storeInBuffer(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		buffer.set(key, value);
		redoLogger.write(new SetMutation(key, value));
		if (buffer.getUsed() > bufferLimit) {
			commit();
		}
	}

	@Override
	public void deleteFromBuffer(byte[] key) {
		// TODO Auto-generated method stub
		buffer.set(key, new Value(null, true));
		redoLogger.write(new DeleteMutation(key));

	}

	@Override
	public long getBufferUsed() {
		return buffer.getUsed();
	}

	/**
	 * Build index for region file
	 */
	protected void buildIndex() {

	}

	private IndexEntry getKeyIndex(byte[] key) {
		return RegionUtil.search(indices, key);
	}

	protected void loadLogger(String file) {
		try {
			LoggerFSInputIterator it = new LoggerFSInputIterator(file);
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

	private class RegionFlushListener implements FlushListener {
		private KeyValueCache oldBuffer;
		private String oldFile;

		public RegionFlushListener(KeyValueCache oldBuffer, String oldFile) {
			super();
			this.oldBuffer = oldBuffer;
			this.oldFile = oldFile;
		}

		@Override
		public void onFlushBegin() {
			oldBuffer = buffer;
			buffer = new KeyValueCache(0, null);
		}

		@Override
		public void onFlushEnd(boolean success, String file) {
			if (success) {
				try {
					indices = IndexBuilder.build(fsclient.open(file), blockSize, indexBlockCount);
				} catch (Exception e) {
					logger.error("Error occured when building index for data file:" + file);
					restore();
					throw new RuntimeException(e);
				}
			} else {
				restore();
			}
		}

		@Override
		public void onFlushCommit(boolean success, String file) {
			if (success) {
				dataFile = file;
			} else {
				restore();
			}
		}

		private void restore() {
			oldBuffer.addAll(buffer);
			buffer = oldBuffer;
			dataFile = oldFile;
		}

	}
}
