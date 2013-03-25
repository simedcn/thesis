package com.ebay.kvstore.server.master.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mina.core.session.IoSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.ServerConstants;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.logger.FileOperationLogger;
import com.ebay.kvstore.server.master.logger.FileOperationLoggerIterator;
import com.ebay.kvstore.server.master.logger.IOperation;
import com.ebay.kvstore.server.master.logger.IOperationLogger;
import com.ebay.kvstore.server.master.logger.SplitOperation;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionTable;

/**
 * TODO
 * 
 * @author luochen
 * 
 */
public class MasterEngine implements IMasterEngine {

	private Logger logger = LoggerFactory.getLogger(MasterEngine.class);

	private Map<DataServerStruct, IoSession> dataServers;

	private Map<Integer, Region> unassignedRegions;

	private Integer nextRegionId = 0;

	private ZooKeeper zk;

	private IOperationLogger opLogger;

	private FileSystem fs;

	private IConfiguration conf;

	private int checkpointInterval;

	private long checkpointTime;

	private Timer checkpointTimer;

	public MasterEngine(IConfiguration conf, ZooKeeper zk) {
		this.conf = conf;
		this.dataServers = new HashMap<>();
		this.unassignedRegions = new HashMap<>();
		this.zk = zk;
		this.fs = DFSManager.getDFS();
		this.checkpointInterval = (int) (this.conf.getDouble(IConfigurationKey.Checkpoint_Interval) * ServerConstants.Day);
	}

	@Override
	public synchronized void addDataServer(DataServerStruct struct, IoSession session) {
		dataServers.put(struct, session);
	}

	@Override
	public void assignRegion(DataServerStruct struct, Region region) {

	}

	@Override
	public void checkSplitRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized DataServerStruct[] getAllDataServers() {
		return dataServers.keySet().toArray(new DataServerStruct[] {});
	}

	@Override
	public synchronized DataServerStruct getDataServer(Address addr) {
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			struct = it.next();
			if (struct.getAddr().equals(addr)) {
				return struct;
			}
		}
		return null;
	}

	@Override
	public synchronized DataServerStruct getDataServer(Region region) {
		for (DataServerStruct struct : dataServers.keySet()) {
			if (struct.containsRegion(region)) {
				return struct;
			}
		}
		return null;
	}

	/**
	 * RegionTable is a snapshot of current region/address mappings. To keep it
	 * simple, we will generate a new RegionTable every time.
	 */
	@Override
	public synchronized RegionTable getRegionTable() {
		RegionTable table = new RegionTable();
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		while (it.hasNext()) {
			DataServerStruct ds = it.next();
			Region[] regions = ds.getAllRegions();
			for (Region region : regions) {
				table.addRegion(region, ds.getAddr());
			}
		}
		return table;
	}

	@Override
	public synchronized void loadRegion(Address addr, Region region) {
		DataServerStruct struct = getDataServer(addr);
		struct.addRegion(region);
		unassignedRegions.remove(region.getRegionId());
	}

	@Override
	public synchronized int nextRegionId() throws Exception {
		zk.setData(ServerConstants.ZooKeeper_Master_Region_Id,
				KeyValueUtil.intToBytes(nextRegionId + 1), -1);
		nextRegionId++;
		return nextRegionId;
	}

	@Override
	public synchronized void removeDataServer(Address addr) {
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			struct = it.next();
			if (struct.getAddr().equals(addr)) {
				it.remove();
			}
		}
		if (struct != null) {
			SortedSet<Region> regions = struct.getRegions();
			Iterator<Region> rit = regions.iterator();
			while (it.hasNext()) {
				Region region = rit.next();
				unassignedRegions.put(region.getRegionId(), region);
			}
			logger.info("remove data server {} from cluster", addr);
		}

	}

	public synchronized void removeDataServer(DataServerStruct ds) {
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			struct = it.next();
			if (struct.getAddr().equals(ds)) {
				it.remove();
			}
		}
	}

	@Override
	public void resetRegionId() throws Exception {
		zk.setData(ServerConstants.ZooKeeper_Master_Region_Id, KeyValueUtil.intToBytes(0), -1);
		nextRegionId = 0;
	}

	@Override
	public synchronized void shutdown() throws IOException {
		if (checkpointTimer != null) {
			checkpointTimer.cancel();
		}
		checkPoint();
	}

	@Override
	public void splitRegion(Address addr, Region oldRegion, Region newRegion) {
		DataServerStruct struct = getDataServer(addr);
		struct.addRegion(oldRegion);
		struct.addRegion(newRegion);
	}

	@Override
	public synchronized void start() throws Exception {
		// init region id
		if (zk.exists(ServerConstants.ZooKeeper_Master_Region_Id, false) == null) {
			zk.create(ServerConstants.ZooKeeper_Master_Region_Id, KeyValueUtil.intToBytes(0),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			nextRegionId = KeyValueUtil.bytesToInt(zk.getData(
					ServerConstants.ZooKeeper_Master_Region_Id, false, null));
		}
		String checkPointDir = PathBuilder.getMasterCheckPointDir();
		String logDir = PathBuilder.getMasterLogDir();
		String[] checkPoints = FSUtil.getMasterCheckpointFiles(checkPointDir);
		String[] logs = FSUtil.getMasterLogFiles(logDir);
		for (String checkpoint : checkPoints) {
			try {
				tryLoad(checkPointDir, checkpoint, logs);
			} catch (Exception e) {
				logger.error("Fail to recover master server from " + checkpoint, e);
			}
		}
		long time = System.currentTimeMillis();
		String log = PathBuilder.getMasterLogPath(time);
		opLogger = FileOperationLogger.forCreate(log);
		// put the default region if there is no region added
		if (unassignedRegions.size() == 0) {
			Region region = new Region(nextRegionId(), new byte[] { 0 }, null);
			unassignedRegions.put(region.getRegionId(), region);
		}

		checkpointTimer = new Timer();
		checkpointTimer.schedule(new CheckpointTask(), checkpointInterval, ServerConstants.Hour);
	}

	@Override
	public synchronized void syncDataServer(DataServerStruct struct, IoSession session) {
		if (struct == null) {
			return;
		}
		Region[] newRegions = struct.getAllRegions();
		DataServerStruct oldStruct = getDataServer(struct.getAddr());
		if (oldStruct == null) {
			addDataServer(struct, session);
			for (Region nr : newRegions) {
				unassignedRegions.remove(nr.getRegionId());
			}
		} else {
			oldStruct.setWeight(struct.getWeight());
			oldStruct.setInfo(struct.getInfo());
			for (Region nr : newRegions) {
				if (unassignedRegions.containsValue(nr)) {
					unassignedRegions.remove(nr.getRegionId());
					oldStruct.addRegion(nr);
				} else if (getDataServer(nr) != null && !oldStruct.containsRegion(nr)) {
					unassignRegion(oldStruct, nr);
				} else {
					oldStruct.addRegion(nr);
				}
			}
		}
	}

	@Override
	public void unassignRegion(DataServerStruct struct, Region region) {

	}

	@Override
	public void unloadRegoin(Address addr, Region region) {
		DataServerStruct struct = getDataServer(addr);
		struct.removeRegion(region);
		unassignedRegions.put(region.getRegionId(), region);
	}

	protected void loadLogger(String log) throws IOException {
		// TODO Auto-generated method stub
		Iterator<IOperation> it = new FileOperationLoggerIterator(log);
		while (it.hasNext()) {
			IOperation op = it.next();
			switch (op.getType()) {
			case IOperation.Load:
				// do nothing
				break;
			case IOperation.Unload:
				// do nothing
				break;
			case IOperation.Split:
				SplitOperation split = (SplitOperation) op;
				Region region = unassignedRegions.get(split.getRegionId());
				byte[] newKeyStart = KeyValueUtil.nextKey(split.getOldKeyEnd());
				Region newRegion = new Region(split.getNewRegionId(), newKeyStart, region.getEnd());
				region.setEnd(split.getOldKeyEnd());
				unassignedRegions.put(split.getNewRegionId(), newRegion);
				break;
			default:
				break;
			}
		}
	}

	protected synchronized void readFromExternal(InputStream in) throws IOException,
			ClassNotFoundException {
		ObjectInput oin = null;
		try {
			oin = new ObjectInputStream(in);
			int num = oin.readInt();
			Region[] regions = null;
			for (int i = 0; i < num; i++) {
				regions = (Region[]) oin.readObject();
				if (regions != null) {
					for (Region region : regions) {
						unassignedRegions.put(region.getRegionId(), region);
					}
				}
			}
		} finally {
			if (oin != null) {
				oin.close();
			}
		}
	}

	protected synchronized void reset() {
		dataServers.clear();
		unassignedRegions.clear();
	}

	protected void tryLoad(String dir, String checkpoint, String[] logs)
			throws ClassNotFoundException, IOException {
		reset();
		long time = FSUtil.getMasterFileTimestamp(checkpoint);
		readFromExternal(fs.open(new Path(dir, checkpoint)));
		for (String log : logs) {
			if (FSUtil.getMasterFileTimestamp(log) >= time) {
				loadLogger(dir + log);
			} else {
				break;
			}
		}
	}

	/**
	 * Used for serialize
	 * 
	 * @param path
	 * @throws IOException
	 */
	protected synchronized void writeToExternal(OutputStream out) throws IOException {
		ObjectOutput oout = null;
		try {
			oout = new ObjectOutputStream(out);
			int num = dataServers.size();
			oout.writeInt(num);
			Region[] regions = null;
			for (DataServerStruct struct : dataServers.keySet()) {
				regions = struct.getAllRegions();
				oout.writeObject(regions);
			}
		} finally {
			if (oout != null) {
				oout.close();
			}
		}
	}

	private synchronized void checkPoint() throws IOException {
		long time = System.currentTimeMillis();
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
			opLogger.close();
			opLogger = FileOperationLogger.forCreate(log);
			logger.info(
					"Create master checkpoint successfully, create new checkpoint file {} and new log file {}",
					path, log);
		}
	}

	private class CheckpointTask extends TimerTask {
		@Override
		public void run() {
			long time = System.currentTimeMillis();
			if (time - checkpointTime > checkpointInterval)
				try {
					checkPoint();
				} catch (IOException e) {
					logger.error("Fail to generate master point", e);
				}
		}
	}

}
