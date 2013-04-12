package com.ebay.kvstore.server.data.storage.task;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.structure.Region;

public class RegionMerger extends BaseRegionTask {
	// region 1 should be less than region2
	private IRegionStorage storage1;

	private IRegionStorage storage2;

	private IRegionMergeListener listener;

	private static Logger logger = LoggerFactory.getLogger(RegionMerger.class);

	public RegionMerger(IConfiguration conf, IRegionStorage storage1, IRegionStorage storage2,
			IRegionMergeListener listener) {
		super(null, conf);
		if (storage1.getRegion().compareTo(storage2.getRegion()) < 0) {
			this.storage1 = storage1;
			this.storage2 = storage2;
		} else {
			this.storage1 = storage2;
			this.storage2 = storage1;
		}
		this.listener = listener;
	}

	@Override
	public void run() {
		IBlockOutputStream out = null;
		RegionTaskPhase state = RegionTaskPhase.Begin;
		String baseDir = storage1.getRegionDir();
		try {
			if (listener != null) {
				listener.onMergeBegin();
			}
			String target = baseDir + String.valueOf(System.currentTimeMillis());
			out = new KVOutputStream(DFSManager.getDFS().create(new Path(target), true), blockSize);
			flushRegion(out, storage1.getBuffer(), storage1.getDataFile());
			flushRegion(out, storage2.getBuffer(), storage2.getDataFile());
			out.close();
			Region region = null;
			if (listener != null) {
				region = listener.onMergeEnd(true, storage1.getRegion(), storage2.getRegion());
			}
			state = RegionTaskPhase.End;
			long time = System.currentTimeMillis();
			String newRegionDir = PathBuilder.getRegionDir(region.getRegionId());
			if (!fs.exists(new Path(newRegionDir))) {
				fs.mkdirs(new Path(newRegionDir));
			}

			String dataFile = baseDir + PathBuilder.getRegionFileName(region.getRegionId(), time);
			String logFile = PathBuilder.getRegionLogPath(region.getRegionId(), time);
			boolean commitSuccess = DFSManager.getDFS()
					.rename(new Path(target), new Path(dataFile));
			storage = new RegionFileStorage(conf, region);
			storage.setDataFile(dataFile);
			storage.newLogger(logFile);
			if (listener != null) {
				listener.onMergeCommit(commitSuccess, storage);
			}
			state = RegionTaskPhase.Commit;
		} catch (IOException e) {
			logger.error("Error Occured when merging file", e);
			if (listener != null) {
				if (state == RegionTaskPhase.Begin) {
					listener.onMergeEnd(false, null, null);
				} else if (state == RegionTaskPhase.End) {
					listener.onMergeCommit(false, null);
				}
			}
		} finally {
			RegionTaskManager.lock = false;
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
			}
		}
	}
}
