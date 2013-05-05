package com.ebay.kvstore.server.data.storage.task;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.KVFileIterator;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.server.util.DFSManager;
import com.ebay.kvstore.server.util.PathBuilder;

public class RegionFlusher extends BaseRegionTask {
	private static Logger logger = LoggerFactory.getLogger(RegionFlusher.class);

	protected IRegionFlushListener listener;

	public RegionFlusher(IRegionStorage storage, IConfiguration conf, IRegionTaskListener listener) {
		super(storage, conf);
		this.listener = (IRegionFlushListener) listener;
	}

	@Override
	public synchronized void run() {
		IBlockOutputStream out = null;
		KVFileIterator fileIt = null;
		RegionTaskPhase state = RegionTaskPhase.Begin;
		String baseDir = storage.getRegionDir();
		String dataFile = storage.getDataFile();
		KeyValueCache buffer = storage.getBuffer();
		try {
			if (listener != null) {
				listener.onFlushBegin();
			}
			String target = baseDir + String.valueOf(System.currentTimeMillis());
			out = new KVOutputStream(DFSManager.getDFS().create(new Path(target), true), blockSize);
			flushRegion(out, buffer, dataFile);
			out.close();
			if (listener != null) {
				listener.onFlushEnd(true, target);
			}
			state = RegionTaskPhase.End;
			String finalFile = baseDir
					+ PathBuilder.getRegionFileName(storage.getRegion().getRegionId(),
							System.currentTimeMillis());
			boolean commitSuccess = DFSManager.getDFS().rename(new Path(target),
					new Path(finalFile));
			if (listener != null) {
				listener.onFlushCommit(commitSuccess, finalFile);
			}
			state = RegionTaskPhase.Commit;
		} catch (IOException e) {
			logger.error("Error Occured when flushing file", e);
			if (listener != null) {
				if (state == RegionTaskPhase.Begin) {
					listener.onFlushEnd(false, null);
				} else if (state == RegionTaskPhase.End) {
					listener.onFlushCommit(false, null);
				}
			}
		} finally {
			RegionTaskManager.lock = false;
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
