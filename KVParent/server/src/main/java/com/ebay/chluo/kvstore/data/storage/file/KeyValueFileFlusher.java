package com.ebay.chluo.kvstore.data.storage.file;

import java.io.File;
import java.io.FileOutputStream;
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

/**
 * 
 * @author luochen
 * 
 */
public class KeyValueFileFlusher implements Runnable {
	/**
	 * Lock for concurrent flushing
	 */
	private static volatile Boolean running = false;

	private static Logger logger = LoggerFactory.getLogger(KeyValueFileFlusher.class);

	public static boolean flush(KeyValueCache cache, File srcFile, Region region, int blockSize,
			FlushListener listener) {
		if (running) {
			return false;
		}
		synchronized (running) {
			if (running) {
				return false;
			}
			Thread t = new Thread(new KeyValueFileFlusher(cache, srcFile, region, blockSize,
					listener));
			t.start();
			running = true;
			return true;
		}
	}

	private KeyValueCache cache;
	private File srcFile;
	private FlushListener listener;
	private int blockSize;
	private Region region;

	public KeyValueFileFlusher(KeyValueCache cache, File srcFile, Region region, int blockSize,
			FlushListener listener) {
		this.cache = cache;
		this.srcFile = srcFile;
		this.listener = listener;
		this.blockSize = blockSize;
		this.region = region;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		KVOutputStream out = null;
		KeyValueInputIterator fileIt = null;
		try {
			if (listener != null) {
				listener.onFlushBegin();
			}
			// Just a temp file
			String target = String.valueOf(System.currentTimeMillis());
			File targetFile = new File(target);
			out = new KVFileOutputStream(new FileOutputStream(targetFile), blockSize);
			Iterator<Entry<byte[], Value>> cacheIt = cache.iterator();
			if (srcFile == null) {
				flushCache(cacheIt, out);
			} else {
				fileIt = new KeyValueInputIterator(0, -1, blockSize, 0, srcFile);
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
				listener.onFlushEnd(true, targetFile);
			}
			target = getRegionFilePath();
			File finalFile = new File(target);
			targetFile.renameTo(new File(target));
			if (listener != null) {
				listener.onFlushCommit(finalFile);
			}
			running = false;
		} catch (IOException e) {
			logger.error("Error Occured when flushing file", e);
			if (listener != null) {
				listener.onFlushEnd(false, null);
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

	private void flushCache(Iterator<Entry<byte[], Value>> it, KVOutputStream out)
			throws IOException {
		Entry<byte[], Value> e = null;
		while (it.hasNext()) {
			e = it.next();
			if (!e.getValue().isDeleted()) {
				KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
			}
		}
	}

	private void flushFile(Iterator<KeyValue> it, KVOutputStream out) throws IOException {
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
