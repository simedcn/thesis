package com.ebay.kvstore.server.data.storage.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.RegionUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.IndexBuilder;
import com.ebay.kvstore.server.data.storage.fs.IndexEntry;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.structure.Region;

public class RegionLoader extends BaseRegionTask {

	private static Logger logger = LoggerFactory.getLogger(RegionLoader.class);

	protected Region region;
	protected int regionId;
	protected IRegionLoadListener listener;
	protected FileSystem fs;
	protected int blockSize;
	protected int indexBlockNum;

	public RegionLoader(IConfiguration conf, IRegionLoadListener listener, Region region) {
		super(null, conf);
		this.listener = listener;
		this.region = region;
		this.regionId = region.getRegionId();
		this.fs = DFSManager.getDFS();
		this.blockSize = conf.getInt(IConfigurationKey.Dataserver_Region_Block_Size);
		this.indexBlockNum = conf.getInt(IConfigurationKey.Dataserver_Region_Index_Block_Num);
	}

	public boolean load() {
		RegionTaskPhase phase = RegionTaskPhase.Begin;
		try {
			String baseDir = PathBuilder.getRegionDir(regionId);
			String[] dataFiles = FSUtil.getRegionFiles(baseDir);
			String[] logFiles = FSUtil.getRegionLogFiles(baseDir);
			String dataFile = null;
			String logFile = null;
			boolean success = false;
			listener.onLoadBegin();
			List<IndexEntry> indices = null;
			KeyValueCache buffer = null;
			if (dataFiles == null || dataFiles.length == 0) {
				if (logFiles == null || logFiles.length == 0) {
					buffer = KeyValueCache.forBuffer();
					success = true;
				} else {
					for (int i = logFiles.length - 1; i >= 0; i--) {
						try {
							buffer = KeyValueCache.forBuffer();
							logFile = logFiles[i];
							RegionUtil.loadLogger(baseDir + logFile, buffer);
							success = true;
						} catch (Exception e) {
							logger.warn("Fail to load region from log file:" + logFiles[i], e);
						}
					}
				}
			} else {
				for (int i = dataFiles.length - 1; i >= 0; i--) {
					dataFile = dataFiles[i];
					try {
						// load success if no exception throws
						LoaderResult result = tryLoad(baseDir, dataFile, logFiles);
						indices = result.indices;
						buffer = result.cache;
						logFile = result.log;
						success = true;
						break;
					} catch (Exception e) {
						logger.warn("Fail to load region from file:" + dataFile, e);
					}
				}
			}
			if (success) {
				listener.onLoadEnd(true);
			} else {
				listener.onLoadEnd(false);
				return false;
			}
			phase = RegionTaskPhase.End;
			// commit

			IRegionStorage storage = null;
			if (dataFile != null) {
				storage = new RegionFileStorage(conf, region, baseDir + dataFile, indices, false);
			} else {
				storage = new RegionFileStorage(conf, region);
			}
			if (logFile == null) {
				long time = System.currentTimeMillis();
				logFile = PathBuilder.getRegionLogPath(region.getRegionId(), time);
				storage.newLogger(logFile);
			}else{
				logFile = PathBuilder.getRegionDir(region.getRegionId())+logFile;
				storage.setLogger(logFile);
			}
			storage.setBuffer(buffer);
			listener.onLoadCommit(success, storage);
			phase = RegionTaskPhase.Commit;
			return true;
		} catch (IOException e) {
			logger.error("Fail to load region:" + regionId, e);
			if (phase == RegionTaskPhase.Begin) {
				listener.onLoadEnd(false);
			} else if (phase == RegionTaskPhase.End) {
				listener.onLoadCommit(false, null);
			}
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
	}

	/**
	 * Try load region from a given region file
	 * 
	 * @param file
	 * @throws IOException
	 */
	private LoaderResult tryLoad(String dir, String file, String[] logFiles) throws IOException {
		List<IndexEntry> indices = new ArrayList<>();
		IndexBuilder.build(indices, dir + file, blockSize, indexBlockNum);
		KeyValueCache buffer = KeyValueCache.forBuffer();
		String log = null;
		// load log files
		long dataTime = FSUtil.getRegionFileTimestamp(file);
		for (int i = logFiles.length - 1; i >= 0; i--) {
			long logTime = FSUtil.getRegionFileTimestamp(logFiles[i]);
			if (logTime >= dataTime) {
				if (log == null) {
					log = logFiles[i];
				}
				RegionUtil.loadLogger(dir + logFiles[i], buffer);
			} else {
				break;
			}
		}
		return new LoaderResult(indices, buffer, log);
	}

	private class LoaderResult {
		public List<IndexEntry> indices;
		public KeyValueCache cache;
		public String log;

		public LoaderResult(List<IndexEntry> indices, KeyValueCache cache, String log) {
			this.indices = indices;
			this.cache = cache;
			this.log = log;
		}

	}

}
