package com.ebay.kvstore.server.data.storage.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
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

public class RegionLoader extends BaseHelper {

	private static Logger logger = LoggerFactory.getLogger(RegionLoader.class);
	protected Region region;
	protected int regionId;
	protected Address addr;
	protected IRegionLoadListener listener;
	protected FileSystem fs;
	protected int blockSize;
	protected int indexBlockNum;

	public RegionLoader(IConfiguration conf, IRegionLoadListener listener, Region region,
			Address addr) {
		super(null, conf);
		this.listener = listener;
		this.region = region;
		this.regionId = region.getRegionId();
		this.addr = addr;
		this.fs = DFSManager.getDFS();
		this.blockSize = conf.getInt(IConfigurationKey.Region_Block_Size);
		this.indexBlockNum = conf.getInt(IConfigurationKey.Region_Index_Block_Num);
	}

	/**
	 * TODO Region Loader
	 * 
	 * @param addr
	 * @param regionId
	 * @throws IOException
	 */
	public void run() {
		Phase phase = Phase.Begin;
		try {
			String baseDir = PathBuilder.getRegionDir(addr, regionId);
			String[] dataFiles = RegionUtil.getRegionFiles(baseDir);
			String[] logFiles = RegionUtil.getRegionLogFiles(baseDir);
			String dataFile = null;
			boolean success = false;
			listener.onLoadBegin();
			List<IndexEntry> indices = null;
			KeyValueCache buffer = null;
			if (dataFiles == null || dataFiles.length == 0) {
				for (int i = logFiles.length - 1; i >= 0; i--) {
					try {
						buffer = KeyValueCache.forBuffer();
						RegionUtil.loadLogger(baseDir + logFiles[i], buffer);
						success = true;
					} catch (Exception e) {
						logger.warn("Fail to load region from log file:" + logFiles[i], e);
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
				return;
			}
			phase = Phase.End;
			// commit
			long time = System.currentTimeMillis();
			String logFile = PathBuilder.getRegionLogPath(addr, region.getRegionId(), time);
			IRegionStorage storage = null;
			if (dataFile != null) {
				String newDataFile = PathBuilder
						.getRegionFilePath(addr, region.getRegionId(), time);
				String dataDir = PathBuilder.getRegionDir(addr, region.getRegionId());
				if (!fs.exists(new Path(dataDir))) {
					fs.mkdirs(new Path(dataDir));
				}
				success = fs.rename(new Path(baseDir, dataFile), new Path(newDataFile));
				storage = new RegionFileStorage(conf, region, newDataFile, indices, false);
			} else {
				storage = new RegionFileStorage(conf, region);
			}
			storage.newLogger(logFile);
			storage.setBuffer(buffer);
			listener.onLoadCommit(success, storage);
			phase = Phase.Commit;
		} catch (Exception e) {
			logger.error("Fail to load region:" + regionId, e);
			if (phase == Phase.Begin) {
				listener.onLoadEnd(false);
			} else if (phase == Phase.End) {
				listener.onLoadCommit(false, null);
			}
			throw new RuntimeException(e);
		}
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
		// load log files
		long dataTime = RegionUtil.getRegionFileTimestamp(file);
		for (int i = logFiles.length - 1; i >= 0; i--) {
			long logTime = RegionUtil.getRegionFileTimestamp(logFiles[i]);
			if (logTime >= dataTime) {
				RegionUtil.loadLogger(dir + logFiles[i], buffer);
			} else {
				break;
			}
		}
		return new LoaderResult(indices, buffer);
	}

	private class LoaderResult {
		public List<IndexEntry> indices;
		public KeyValueCache cache;

		public LoaderResult(List<IndexEntry> indices, KeyValueCache cache) {
			this.indices = indices;
			this.cache = cache;
		}

	}

}
