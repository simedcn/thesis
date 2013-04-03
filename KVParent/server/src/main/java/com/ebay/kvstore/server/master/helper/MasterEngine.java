package com.ebay.kvstore.server.master.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mina.core.session.IoSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.FSUtil;
import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.ServerConstants;
import com.ebay.kvstore.exception.InvalidDataServerException;
import com.ebay.kvstore.exception.InvalidRegionException;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.request.UnloadRegionRequest;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.logger.FileOperationLogger;
import com.ebay.kvstore.server.master.logger.FileOperationLoggerIterator;
import com.ebay.kvstore.server.master.logger.IOperation;
import com.ebay.kvstore.server.master.logger.IOperationLogger;
import com.ebay.kvstore.server.master.logger.SplitOperation;
import com.ebay.kvstore.server.master.task.IMasterTask;
import com.ebay.kvstore.server.master.task.MasterTaskManager;
import com.ebay.kvstore.server.master.task.RegionAssignTask;
import com.ebay.kvstore.structure.Address;
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

	// Map data server address to data client address
	private BidiMap dsClientMapping;

	private Map<DataServerStruct, IoSession> dataServers;

	private Map<Integer, Region> unassignedRegions;

	private Integer nextRegionId = 0;

	private ZooKeeper zk;

	private IOperationLogger opLogger;

	private FileSystem fs;

	private IConfiguration conf;

	private RegionAssignTask assigner;

	private MasterEngineListenerManager listenerManager;

	private MasterTaskManager taskManager;

	public MasterEngine(IConfiguration conf, ZooKeeper zk) {
		this.conf = conf;
		this.dataServers = new HashMap<>();
		this.dsClientMapping = new DualHashBidiMap();
		this.unassignedRegions = new HashMap<>();
		this.zk = zk;
		this.fs = DFSManager.getDFS();
		this.listenerManager = new MasterEngineListenerManager();
		this.taskManager = new MasterTaskManager();
	}

	@Override
	public synchronized void addDataServer(DataServerStruct struct, IoSession session) {
		dsClientMapping.put(struct.getAddr(), Address.parse(session.getRemoteAddress()));
		dataServers.put(struct, session);
	}

	@Override
	public void checkSplitRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean containsDataClient(Address addr) {
		return dsClientMapping.containsValue(addr);
	}

	@Override
	public boolean containsDataServer(Address addr) {
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			struct = it.next();
			if (struct.getAddr().equals(addr)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public synchronized Collection<DataServerStruct> getAllDataServers() {
		return dataServers.keySet();
	}

	@Override
	public DataServerStruct getDataServer(Address addr) {
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
	public synchronized DataServerStruct getDataServer(Region region) throws InvalidRegionException {
		for (DataServerStruct struct : dataServers.keySet()) {
			if (struct.containsRegion(region)) {
				return struct;
			}
		}
		throw new InvalidRegionException("Region with " + region
				+ " does not exists in the cluster");
	}

	@Override
	public synchronized DataServerStruct getDataServerByClient(Address addr)
			throws InvalidDataServerException {
		Address serverAddr = (Address) dsClientMapping.getKey(addr);
		if (serverAddr == null) {
			throw new InvalidDataServerException("Data Server with client" + addr
					+ " does not exists in the cluster");
		}
		Iterator<DataServerStruct> it = dataServers.keySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			struct = it.next();
			if (struct.getAddr().equals(serverAddr)) {
				return struct;
			}
		}
		return null;

	}

	@Override
	public IoSession getDataServerConnection(Address addr) throws InvalidDataServerException {
		Iterator<Entry<DataServerStruct, IoSession>> it = dataServers.entrySet().iterator();
		while (it.hasNext()) {
			Entry<DataServerStruct, IoSession> e = it.next();
			if (e.getKey().getAddr().equals(addr)) {
				return e.getValue();
			}
		}
		throw new InvalidDataServerException("Data Server with " + addr
				+ " does not exists in the cluster");
	}

	@Override
	public IoSession getDataServerConnectionByClient(Address clientAddr)
			throws InvalidDataServerException {
		Address serverAddr = (Address) dsClientMapping.getKey(clientAddr);
		if (serverAddr == null) {
			throw new InvalidDataServerException("Data Server with client" + clientAddr
					+ " does not exists in the cluster");
		}
		return dataServers.get(new DataServerStruct(serverAddr, 0));
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
			Collection<Region> regions = ds.getRegions();
			for (Region region : regions) {
				table.addRegion(region, ds.getAddr());
			}
		}
		return table;
	}

	@Override
	public Map<Integer, Region> getUnassignedRegions() {
		return unassignedRegions;
	}

	@Override
	public synchronized void loadRegion(DataServerStruct struct, Region region)
			throws InvalidDataServerException {
		struct.addRegion(region);
		unassignedRegions.remove(region.getRegionId());
		listenerManager.onRegionLoad(struct, region);
	}

	@Override
	public synchronized int nextRegionId() {
		try {
			zk.setData(ServerConstants.ZooKeeper_Master_Region_Id,
					KeyValueUtil.intToBytes(nextRegionId + 1), -1);
		} catch (Exception e) {
			logger.error("Fail store next region id to zookeeper", e);
		}
		nextRegionId++;
		return nextRegionId;
	}

	@Override
	public void registerListener(IMasterEngineListener listener) {
		listenerManager.registerListener(listener);
	}

	@Override
	public void registerTask(IMasterTask task, boolean listener) {
		taskManager.registerTask(task);
		if (listener) {
			listenerManager.registerListener((IMasterEngineListener) task);
		}
	}

	@Override
	public synchronized void removeDataServer(IoSession session) {
		Iterator<Entry<DataServerStruct, IoSession>> it = dataServers.entrySet().iterator();
		DataServerStruct struct = null;
		while (it.hasNext()) {
			Entry<DataServerStruct, IoSession> e = it.next();
			if (e.getValue().equals(session)) {
				struct = e.getKey();
				it.remove();
				break;
			}
		}
		if (struct != null) {
			dsClientMapping.remove(struct.getAddr());
			Collection<Region> regions = struct.getRegions();
			Iterator<Region> rit = regions.iterator();
			while (rit.hasNext()) {
				Region region = rit.next();
				unassignedRegions.put(region.getRegionId(), region);
			}
			listenerManager.onDataServerUnload(struct);
			logger.info("remove data server {} from cluster", struct.getAddr());
		}
	}

	@Override
	public void resetRegionId() throws Exception {
		zk.setData(ServerConstants.ZooKeeper_Master_Region_Id, KeyValueUtil.intToBytes(0), -1);
		nextRegionId = 0;
	}

	@Override
	public void setLogger(String path) {
		try {
			if (opLogger != null) {
				opLogger.close();
			}

			opLogger = FileOperationLogger.forCreate(path);
		} catch (IOException e) {
			logger.error("Fail to create new logger " + path, e);
		}

	}

	@Override
	public void splitRegion(DataServerStruct struct, Region oldRegion, Region newRegion)
			throws InvalidDataServerException {
		struct.addRegion(oldRegion);
		struct.addRegion(newRegion);
		listenerManager.onRegionSplit(oldRegion, newRegion);
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
		taskManager.start();
	}

	@Override
	public synchronized void stop() throws IOException {
		taskManager.stop();
	}

	@Override
	public synchronized void syncDataServer(DataServerStruct struct, IoSession session)
			throws KVException {
		if (struct == null) {
			return;
		}
		Collection<Region> newRegions = struct.getRegions();
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
					loadRegion(oldStruct, nr);
				}
			}
		}
	}

	@Override
	public void unloadRegoin(DataServerStruct struct, Region region)
			throws InvalidDataServerException {
		struct.removeRegion(region);
		unassignedRegions.put(region.getRegionId(), region);
		listenerManager.onRegionUnload(struct, region);
	}

	@Override
	public void unregisterListener(IMasterEngineListener listener) {
		listenerManager.unregisterListener(listener);
	}

	@Override
	public void unregisterTask(IMasterTask task) {
		taskManager.unregisterTask(task);
		if (task instanceof IMasterEngineListener) {
			listenerManager.unregisterListener((IMasterEngineListener) task);
		}
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
		listenerManager.unregisterAll();
		taskManager.unregisterAll();
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

	private void unassignRegion(DataServerStruct struct, Region region) {
		IoSession session = dataServers.get(struct);
		session.write(new UnloadRegionRequest(region.getRegionId()));
	}
}
