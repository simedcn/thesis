package com.ebay.kvstore.server.master.task;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.ServerConstants;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class CheckpointTask extends BaseMasterTask {

	private static Logger logger = LoggerFactory.getLogger(CheckpointTask.class);

	protected FileSystem fs = DFSManager.getDFS();

	protected long checkpointTime = 0;

	protected long checkpointInterval;

	public CheckpointTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.checkpointInterval = conf.getDouble(IConfigurationKey.Master_Checkpoint_Interval)
				.longValue();
		this.interval = (int) (checkpointInterval / 10);
	}

	@Override
	protected void process() {
		try {
			long time = System.currentTimeMillis();
			if (time - checkpointTime < checkpointInterval) {
				return;
			}
			synchronized (engine) {
				logger.info("Start to create master checkpoint in {}", time);
				String dir = PathBuilder.getMasterCheckPointDir();
				String tmpPath = dir + time;
				String path = PathBuilder.getMasterCheckPointPath(time);
				String log = PathBuilder.getMasterLogPath(time);
				writeToExternal(fs.create(new Path(tmpPath)));
				// rename(commit)
				boolean success = fs.rename(new Path(tmpPath), new Path(path));
				if (success) {
					checkpointTime = time;
					engine.setLogger(log);
					logger.info(
							"Create master checkpoint successfully, create new checkpoint file {} and new log file {}",
							path, log);
				}
			}
		} catch (IOException e) {
			logger.error("Fail to create master checkpoint", e);
		}
	}

	/**
	 * Used for serialize
	 * 
	 * @param path
	 * @throws IOException
	 */
	protected synchronized void writeToExternal(OutputStream out) throws IOException {
		synchronized (engine) {
			ObjectOutput oout = null;
			try {
				Collection<DataServerStruct> dataServers = engine.getAllDataServers();
				oout = new ObjectOutputStream(out);
				int num = dataServers.size();
				oout.writeInt(num);
				Region[] regions = null;
				for (DataServerStruct struct : dataServers) {
					regions = struct.getRegions().toArray(new Region[] {});
					oout.writeObject(regions);
				}
			} finally {
				if (oout != null) {
					oout.close();
				}
			}
		}
	}

}
