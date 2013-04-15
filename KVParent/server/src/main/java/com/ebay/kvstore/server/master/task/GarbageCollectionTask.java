package com.ebay.kvstore.server.master.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.engine.IMasterEngine;

public class GarbageCollectionTask extends BaseMasterTask {
	private static Logger logger = LoggerFactory.getLogger(GarbageCollectionTask.class);

	private FileSystem fs;

	private int checkpointReserve;

	private int regionReserve;

	private int tmpReserve;

	private long current;

	public GarbageCollectionTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.fs = DFSManager.getDFS();
		this.interval = conf.getInt(IConfigurationKey.Master_Gc_Check_Interval);
		this.checkpointReserve = conf.getDouble(IConfigurationKey.Master_Checkpoint_Reserve_Days)
				.intValue();
		this.regionReserve = conf.getDouble(IConfigurationKey.DataServer_Region_Reserve_Days)
				.intValue();
		this.tmpReserve = conf.getDouble(IConfigurationKey.Tmp_File_Reserve_Days).intValue();
	}

	@Override
	protected void process() {
		current = System.currentTimeMillis();
		checkMasterDir();
		checkDataDir();
	}

	private void checkMasterDir() {
		try {
			String checkPointBase = PathBuilder.getMasterCheckPointDir();
			Path checkPointPath = new Path(checkPointBase);
			List<String> checkpoints = FSUtil.listFiles(checkPointBase);
			for (String checkpoint : checkpoints) {
				checkMasterCheckpointFile(checkPointPath, checkpoint);
			}
		} catch (Exception e) {
			logger.error("Error occured check master checkpoint dir", e);
		}
		try {
			String logBase = PathBuilder.getMasterLogDir();
			Path logPath = new Path(logBase);
			List<String> logs = FSUtil.listFiles(logBase);
			for (String log : logs) {
				checkMasterLogFile(logPath, log);
			}
		} catch (Exception e) {
			logger.error("Error occured check master log dir", e);
		}
	}

	private void checkDataDir() {
		try {
			String base = PathBuilder.getDataServerDir();
			List<String> regions = FSUtil.listFiles(base);
			for (String region : regions) {
				checkRegion(base, region);
			}
		} catch (IOException e) {
			logger.error("Error occured when list data dirs", e);
		}
	}

	private void checkRegion(String baseDir, String region) {
		String regionDir = baseDir + region;
		try {
			int regionId = Integer.valueOf(region);
			Path regionPath = new Path(regionDir);
			List<String> files = FSUtil.listFiles(regionDir);
			if (!engine.containsRegion(regionId) && files.size() == 0) {
				fs.delete(regionPath, true);
				return;
			}
			checkDataServerRegionFile(regionPath, files);
		} catch (NumberFormatException e) {
			logger.info("Find invalid region dir:{}, try to delete it", regionDir);
			try {
				fs.delete(new Path(regionDir), true);
			} catch (IOException ioe) {
				logger.info("Fail to delete region dir:" + regionDir, ioe);
			}
		} catch (Exception e) {
			logger.error("Error occured when check region Dir:" + regionDir, e);
		}
	}

	private void checkMasterCheckpointFile(Path base, String checkpoint) {
		Path ckPath = new Path(base, checkpoint);
		try {
			boolean delete = false;
			if (FSUtil.isValidCheckpointFile(checkpoint)) {
				long time = FSUtil.getRegionFileTimestamp(checkpoint);
				if (current - time > checkpointReserve) {
					delete = true;
				}
			} else {
				long time = FSUtil.getFileTimestamp(ckPath);
				if (current - time > tmpReserve) {
					delete = true;
				}
			}
			if (delete) {
				fs.delete(ckPath, true);
			}
		} catch (Exception e) {
			logger.error("Error occured when check master checkpoint File:" + ckPath, e);
		}
	}

	private void checkMasterLogFile(Path base, String log) {
		Path logPath = new Path(base, log);
		try {
			boolean delete = false;
			if (FSUtil.isValidMasterLogFile(log)) {
				long time = FSUtil.getRegionFileTimestamp(log);
				if (current - time > checkpointReserve) {
					delete = true;
				}
			} else {
				long time = FSUtil.getFileTimestamp(logPath);
				if (current - time > tmpReserve) {
					delete = true;
				}
			}
			if (delete) {
				fs.delete(logPath, true);
			}
		} catch (Exception e) {
			logger.error("Error occured when check master log file:" + logPath, e);
		}
	}

	private void checkDataServerRegionFile(Path base, List<String> files) {
		Collections.sort(files);
		List<String> regions = new ArrayList<>();
		List<String> logs = new ArrayList<>();

		for (String file : files) {
			if (FSUtil.isValidRegionFile(file)) {
				regions.add(file);
			} else if (FSUtil.isValidRegionLogFile(file)) {
				logs.add(file);
			} else {
				try {
					Path path = new Path(base, file);
					long time = FSUtil.getFileTimestamp(path);
					if (current - time > tmpReserve) {
						fs.delete(path, true);
					}
				} catch (Exception e) {
					logger.error("Error occured when check file:" + file, e);
				}
			}
		}

		for (int i = regions.size() - 1; i >= 0; i--) {
			String region = regions.get(i);
			try {
				long time = FSUtil.getRegionFileTimestamp(region);
				if (current - time > regionReserve) {
					fs.delete(new Path(base, region), true);
				} else {
					break;
				}
			} catch (Exception e) {
				logger.error("Error occured when check region file:" + region, e);
			}
		}
		for (int i = logs.size() - 1; i >= 0; i--) {
			String log = logs.get(i);
			try {
				long time = FSUtil.getRegionFileTimestamp(log);
				if (current - time > regionReserve) {
					fs.delete(new Path(base, log), true);
				} else {
					break;
				}
			} catch (Exception e) {
				logger.error("Error occured when check log file:" + log, e);
			}
		}
	}

}
