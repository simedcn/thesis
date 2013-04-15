package com.ebay.kvstore.server.master;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.IKVConstants;
import com.ebay.kvstore.MinaUtil;
import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.server.master.engine.MasterEngine;
import com.ebay.kvstore.server.master.handler.DataServerJoinRequestHandler;
import com.ebay.kvstore.server.master.handler.HeartBeatHandler;
import com.ebay.kvstore.server.master.handler.LoadRegionResponseHandler;
import com.ebay.kvstore.server.master.handler.MergeRegionResponseHandler;
import com.ebay.kvstore.server.master.handler.RegionTableRequestHandler;
import com.ebay.kvstore.server.master.handler.SplitRegionResponseHandler;
import com.ebay.kvstore.server.master.handler.StatRequestHandler;
import com.ebay.kvstore.server.master.handler.UnloadRegionResponseHandler;
import com.ebay.kvstore.server.master.task.CheckpointTask;
import com.ebay.kvstore.server.master.task.GarbageCollectionTask;
import com.ebay.kvstore.server.master.task.RegionAssignTask;
import com.ebay.kvstore.server.master.task.RegionMergeTask;
import com.ebay.kvstore.server.master.task.RegionSplitTask;
import com.ebay.kvstore.server.master.task.RegionUnassignTask;
import com.ebay.kvstore.structure.Address;

public class MasterServer implements IConfigurationKey, Watcher {
	private static Logger logger = LoggerFactory.getLogger(MasterServer.class);

	public static void main(String[] args) {
		try {
			IConfiguration conf = ConfigurationLoader.load();
			final MasterServer server = new MasterServer(conf);
			if (args.length >= 1) {
				if (args[0].equals("-format")) {
					server.format();
					return;
				}
			}
			// server.format();

			server.start();

		} catch (Exception e) {
			logger.error("Fail to start master server", e);
		}
	}

	private Address masterAddr;

	private Address zkAddr;
	private Address hdfsAddr;
	private IoAcceptor acceptor;
	private ZooKeeper zooKeeper;
	private int zkSessionTimeout;
	private boolean active = false;
	private String znode;
	private IMasterEngine engine;
	private IConfiguration conf;

	private ProtocolDispatcher dispatcher;

	private int clientSessionTimeout;

	public MasterServer(IConfiguration conf) throws IOException {
		this.conf = conf;
		masterAddr = Address.parse(conf.get(Master_Addr));
		zkAddr = Address.parse(conf.get(ZooKeeper_Addr));
		hdfsAddr = Address.parse(conf.get(Hdfs_Addr));
		zkSessionTimeout = conf.getInt(Zookeeper_Session_Timeout);
		clientSessionTimeout = conf.getInt(Master_Client_Session_Timeout);
		dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(IProtocolType.Heart_Beart_Req, new HeartBeatHandler());
		dispatcher.registerHandler(IProtocolType.Region_Table_Req, new RegionTableRequestHandler());
		dispatcher.registerHandler(IProtocolType.Stat_Req, new StatRequestHandler());
		dispatcher.registerHandler(IProtocolType.Load_Region_Resp, new LoadRegionResponseHandler());
		dispatcher.registerHandler(IProtocolType.Unload_Region_Resp,
				new UnloadRegionResponseHandler());
		dispatcher.registerHandler(IProtocolType.Split_Region_Resp,
				new SplitRegionResponseHandler());
		dispatcher.registerHandler(IProtocolType.DataServer_Join_Request,
				new DataServerJoinRequestHandler());
		dispatcher.registerHandler(IProtocolType.Merge_Region_Resp,
				new MergeRegionResponseHandler());
	}

	public void format() {
		try {
			logger.info("start format cluster");
			initHdfs();
			FileSystem fs = DFSManager.getDFS();
			fs.delete(new Path(IKVConstants.DFS_Base_Dir), true);
			zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, this);
			zooKeeper.delete(IKVConstants.ZooKeeper_Master_Region_Id, -1);
			zooKeeper.close();
		} catch (Exception e) {
			logger.error("Fail to format cluster", e);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
			if (!event.getPath().equals(znode)) {
				logger.info("Node {} deleted. Need to run through the election process.",
						event.getPath());
				try {
					leaderElection();
					if (active == true) {
						run();
					}
				} catch (Exception e) {
					logger.error("Error occured to elect leader after previous node fails", e);
				}
			}
		}
	}

	public synchronized void start() throws Exception {
		initZookeeper();
		if (active) {

			run();
		} else {
			synchronized (zooKeeper) {
				zooKeeper.wait();
			}
		}
	}

	public synchronized void stop() {
		if (acceptor != null) {
			acceptor.unbind();
			acceptor.dispose();
			acceptor = null;
		}
		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
			}
			zooKeeper = null;
		}
		if (engine != null) {
			try {
				engine.stop();
			} catch (IOException e) {
				logger.error("Error occured when stop master engine", e);
			}
		}
	}

	private void initEngine() throws Exception {
		engine = new MasterEngine(conf, zooKeeper);
		engine.registerTask(new CheckpointTask(conf, engine), false);
		engine.registerTask(new GarbageCollectionTask(conf, engine), false);
		engine.registerTask(new RegionAssignTask(conf, engine), true);
		engine.registerTask(new RegionSplitTask(conf, engine), true);
		engine.registerTask(new RegionUnassignTask(conf, engine), true);
		engine.registerTask(new RegionMergeTask(conf, engine), true);
		engine.start();
	}

	private void initHdfs() throws IOException {
		DFSManager.init(hdfsAddr.toInetSocketAddress(), new Configuration());
	}

	private void initServer() throws IOException {
		acceptor = MinaUtil.getDefaultAcceptor();
		acceptor.setHandler(new MasterServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(masterAddr.toInetSocketAddress());
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, this);
		// create the path recursively...
		if (zooKeeper.exists(IKVConstants.ZooKeeper_Master_Dir, false) == null) {
			zooKeeper.create(IKVConstants.ZooKeeper_Base, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			zooKeeper.create(IKVConstants.ZooKeeper_Master_Dir, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}

		// leader election
		znode = zooKeeper.create(IKVConstants.ZooKeeper_Master_Dir_Path, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.info("{} node created in ZooKeeper", znode);
		int index = znode.lastIndexOf('/');
		znode = znode.substring(index + 1);
		leaderElection();
	}

	private void leaderElection() throws KeeperException, InterruptedException {
		List<String> masters = zooKeeper.getChildren(IKVConstants.ZooKeeper_Master_Dir, false);
		Collections.sort(masters);
		for (int i = 0; i < masters.size(); i++) {
			if (masters.get(i).equals(znode)) {
				if (i == 0) {
					active = true;// become leader
				} else {
					zooKeeper.exists(IKVConstants.ZooKeeper_Master_Dir_Path + masters.get(i - 1),
							true);
				}
				break;
			}
		}
	}

	private void run() throws Exception {
		zooKeeper.create(IKVConstants.ZooKeeper_Master_Addr, (masterAddr.toString()).getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				MasterServer.this.stop();
			}
		});
		initHdfs();
		initEngine();
		initServer();
		logger.info("{} become master now, waiting for connections", masterAddr);
	}

	private class MasterServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(MasterServerHandler.class);

		@Override
		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
		}

		@Override
		public void messageReceived(IoSession session, Object obj) throws Exception {
			logger.info("Message received from " + session.getRemoteAddress().toString() + " "
					+ obj);
			try {
				IContext context = new MasterContext(engine, session, conf);
				dispatcher.handle(obj, context);
			} catch (Exception e) {
				logger.error("Error occured when processing message from "
						+ session.getRemoteAddress().toString(), e);
			}
		}

		@Override
		public void messageSent(IoSession session, Object arg1) throws Exception {

		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			engine.removeDataServer(session);
		}

		@Override
		public void sessionCreated(IoSession session) throws Exception {
		}

		@Override
		public void sessionIdle(IoSession session, IdleStatus arg1) throws Exception {

		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {
			int timeout = session.getConfig().getBothIdleTime();
			if (timeout <= 0 || timeout > clientSessionTimeout) {
				session.getConfig().setBothIdleTime(clientSessionTimeout);
			}
		}
	}
}
