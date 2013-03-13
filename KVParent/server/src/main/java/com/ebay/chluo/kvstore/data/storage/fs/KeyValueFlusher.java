package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

interface FlushListener {
	public void onFlushBegin();

	public void onFlushEnd(boolean success, String path);

	public void onFlushCommit(boolean success, String path);
}

/**
 * 
 * @author luochen
 * 
 */
public class KeyValueFlusher implements Runnable {
	/**
	 * Lock for concurrent flushing
	 */
	private static volatile Boolean running = false;

	private static Logger logger = LoggerFactory.getLogger(KeyValueFlusher.class);

	public static boolean isRunning() {
		return running;
	}

	public static boolean flush(KeyValueCache cache, String baseDir, String srcFile, Region region,
			int blockSize, FlushListener listener) {
		if (running) {
			return false;
		}
		synchronized (running) {
			if (running) {
				return false;
			}
			Thread t = new Thread(new KeyValueFlusher(cache, baseDir, srcFile, region, blockSize,
					listener));
			t.start();
			running = true;
			return true;
		}
	}

	private KeyValueCache cache;
	private String srcFile;
	private String baseDir;
	private FlushListener listener;
	private int blockSize;
	private Region region;

	private final int State_Begin = 0;
	private final int State_End = 0;
	private final int State_Commit = 0;

	public KeyValueFlusher(KeyValueCache cache, String baseDir, String srcFile, Region region,
			int blockSize, FlushListener listener) {
		this.cache = cache;
		this.baseDir = baseDir;
		this.srcFile = srcFile;
		this.listener = listener;
		this.blockSize = blockSize;
		this.region = region;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		BlockOutputStream out = null;
		KVFSInputIterator fileIt = null;
		int state = State_Begin;
		try {
			if (listener != null) {
				listener.onFlushBegin();
			}
			// Just a temp file
			String target = baseDir + String.valueOf(System.currentTimeMillis());
			out = new KVOutputStream(DFSClientManager.getClient().create(target, true), blockSize);
			Iterator<Entry<byte[], Value>> cacheIt = cache.iterator();
			if (srcFile == null) {
				flushCache(cacheIt, out);
			} else {
				fileIt = new KVFSInputIterator(0, -1, blockSize, 0, new FileInputStream(srcFile));
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
						KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
						e = null;
					}
				}
				flushCache(cacheIt, out);
				flushFile(fileIt, out);
			}
			out.close();
			if (listener != null) {
				listener.onFlushEnd(true, target);
			}
			state = State_End;
			String finalFile = baseDir + getRegionFilePath();
			boolean commitSuccess = DFSClientManager.getClient().rename(target, finalFile);
			if (listener != null) {
				listener.onFlushCommit(commitSuccess, finalFile);
			}
			state = State_Commit;
			running = false;
		} catch (IOException e) {
			logger.error("Error Occured when flushing file", e);
			if (listener != null) {
				if (state == State_Begin) {
					listener.onFlushEnd(false, null);
				} else if (state == State_End) {
					listener.onFlushCommit(false, null);
				}
			}
		} finally {
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

	private void flushCache(Iterator<Entry<byte[], Value>> it, BlockOutputStream out)
			throws IOException {
		Entry<byte[], Value> e = null;
		while (it.hasNext()) {
			e = it.next();
			if (!e.getValue().isDeleted()) {
				KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
			}
		}
	}

	private void flushFile(Iterator<KeyValue> it, BlockOutputStream out) throws IOException {
		KeyValue kv = null;
		while (it.hasNext()) {
			kv = it.next();
			KeyValueUtil.writeToExternal(out, kv);
		}
	}

	private Object nextEntry(Iterator it) {
		if (it.hasNext()) {
			return it.next();
		} else {
			return null;
		}
	}

	private String getRegionFilePath() {
		return region.getRegionId() + "-" + System.currentTimeMillis() + ".data";
	}
}
