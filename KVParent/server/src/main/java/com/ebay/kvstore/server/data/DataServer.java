package com.ebay.kvstore.server.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.IKVConstants;
import com.ebay.kvstore.MinaUtil;
import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.protocol.request.DataServerJoinRequest;
import com.ebay.kvstore.server.data.handler.DeleteRequestHandler;
import com.ebay.kvstore.server.data.handler.GetRequestHandler;
import com.ebay.kvstore.server.data.handler.IncrRequestHandler;
import com.ebay.kvstore.server.data.handler.SetRequestHandler;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.server.data.storage.IStoreEngineListener;
import com.ebay.kvstore.server.data.storage.StoreEngineFactory;
import com.ebay.kvstore.server.data.storage.StoreLogListener;
import com.ebay.kvstore.server.data.storage.StoreStatListener;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;

public class DataServer implements IConfigurationKey, Watcher {
	private static Logger logger = LoggerFactory.getLogger(DataServer.class);

	public static void main(String[] args) {
		DataServer server = null;
		try {
			IConfiguration conf = ConfigurationLoader.load();
			server = new DataServer(conf);
			server.start();
		} catch (Exception e) {
			logger.error("Fail to start data server", e);
			server.stop();
		}
	}

	private Address dsAddr;
	private Address hdfsAddr;
	private Address zkAddr;
	private IoAcceptor acceptor;
	private ZooKeeper zooKeeper;
	private int zkSessionTimeout;
	private DataClient client;
	private ProtocolDispatcher dispatcher;
	private IConfiguration conf;
	private HeartBeater heartBeater;
	private IStoreEngine engine;
	private int reconnectInteval;
	private int reconnectRetry;
	private int weight;
	private int clientSessionTimeout;// in s

	public DataServer(IConfiguration conf) throws IOException {
		this.conf = conf;
		dsAddr = Address.parse(conf.get(Dataserver_Addr));
		zkAddr = Address.parse(conf.get(ZooKeeper_Addr));
		hdfsAddr = Address.parse(conf.get(Hdfs_Addr));
		zkSessionTimeout = conf.getInt(Zookeeper_Session_Timeout);
		reconnectInteval = conf.getInt(IConfigurationKey.Dataserver_Reconnect_Interval);
		reconnectRetry = conf.getInt(IConfigurationKey.Dataserver_Reconnect_Retry_Count);
		weight = conf.getInt(IConfigurationKey.Dataserver_Weight);
		clientSessionTimeout = conf.getInt(IConfigurationKey.DataServer_Client_Session_Timeout);
		dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(IProtocolType.Set_Req, new SetRequestHandler());
		dispatcher.registerHandler(IProtocolType.Get_Req, new GetRequestHandler());
		dispatcher.registerHandler(IProtocolType.Delete_Req, new DeleteRequestHandler());
		dispatcher.registerHandler(IProtocolType.Incr_Req, new IncrRequestHandler());
	}

	/**
	 * Zookeeper Watcher
	 */
	@Override
	public void process(WatchedEvent event) {
		if (event.getPath() != null && event.getPath().equals(IKVConstants.ZooKeeper_Master_Addr)) {
			if (event.getType().equals(EventType.NodeDeleted)) {
				// try to connect new master server if the master addr node
				// is removed
				logger.error("Master server fails");
				try {
					initConnection();
				} catch (Exception e) {
					logger.error("error occured when setting watcher on master addr", e);
				}
			}
		}
	}

	public void start() throws Exception {
		initHDFS();
		initZookeeper();
		initServer();
		initStoreEngine();
		initConnection();
	}

	public void stop() {
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
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
			}
			client = null;
		}
	}

	private void initConnection() throws KeeperException, InterruptedException {
		try {
			if (heartBeater != null) {
				heartBeater.stop();
			}
			if (client != null) {
				client.close();
			}
		} catch (IOException e) {
			logger.error("error occured when closing existing connection", e);
		}
		int retry = reconnectRetry;
		if (retry <= 0) {
			retry = Integer.MAX_VALUE;
		}
		boolean success = false;
		IoSession session = null;
		DataServerStruct struct = new DataServerStruct(dsAddr, weight);
		struct.addRegion(engine.getAllRegions());
		IProtocol request = new DataServerJoinRequest(struct);
		for (int i = 1; i <= retry; i++) {
			try {
				logger.info("Try to connect to master server, for " + i + " times");
				byte[] data = null;
				if (zooKeeper.exists(IKVConstants.ZooKeeper_Master_Addr, false) == null
						|| (data = zooKeeper.getData(IKVConstants.ZooKeeper_Master_Addr, false,
								null)) == null) {
					continue;
				}
				String masterAddr = new String(data);
				client = new DataClient(Address.parse(masterAddr), conf, engine);
				session = client.connect();
				session.write(request);
				synchronized (engine) {
					engine.wait();
				}
				success = (boolean) session.getAttribute("success", false);
				if (success) {
					logger.info("Connect to master server:" + masterAddr + " successfully");
					break;
				} else {
					logger.error("Fail to connect master server, reason:{}",
							session.getAttribute("code"));
				}
			} catch (Exception e) {
				logger.info("Fail to connect to master server", e);
			} finally {
				Thread.sleep(reconnectInteval);
			}
		}
		if (success) {
			zooKeeper.getData(IKVConstants.ZooKeeper_Master_Addr, true, null);
			heartBeater = new HeartBeater(conf, engine, session);
			heartBeater.start();
		} else {
			logger.error("Fail to connect master server within " + retry
					+ " times. Data server will shutdown.");
			System.exit(-1);
		}
	}

	private void initHDFS() throws IOException {
		DFSManager.init(hdfsAddr.toInetSocketAddress(), new Configuration());
	}

	private void initServer() throws IOException {
		acceptor = MinaUtil.getDefaultAcceptor();
		acceptor.setHandler(new DataServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(dsAddr.toInetSocketAddress());
	}

	private void initStoreEngine() throws IOException {
		engine = StoreEngineFactory.createStoreEngine(conf);
		engine.registerListener(new StoreLogListener());
		engine.registerListener(new StoreStatListener());
		String[] listeners = conf.getArray(IConfigurationKey.Dataserver_Store_Listener);
		if (listeners != null) {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			for (String listener : listeners) {
				try {
					Class clazz = loader.loadClass(listener);
					IStoreEngineListener iListener = (IStoreEngineListener) clazz.getConstructor()
							.newInstance();
					engine.registerListener(iListener);
					logger.info("Register listener {} to store engine.", listener);
				} catch (Exception e) {
					logger.error("Error occured when register listener " + listener, e);
				}
			}
		}

	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, this);
	}

	private class DataServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(DataServerHandler.class);

		@Override
		public void exceptionCaught(IoSession session, Throwable error) throws Exception {

		}

		@Override
		public void messageReceived(IoSession session, Object obj) throws Exception {
			logger.info("Message received from " + session.getRemoteAddress().toString() + " "
					+ obj);
			try {
				IContext context = new DataServerContext(engine, session);
				dispatcher.handle(obj, context);
			} catch (Exception e) {
				logger.error("Error occured when processing message from "
						+ session.getRemoteAddress().toString(), e);
			}

		}

		@Override
		public void messageSent(IoSession session, Object obj) throws Exception {

		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
		}

		@Override
		public void sessionCreated(IoSession session) throws Exception {
			System.out.println("Session created " + session.getRemoteAddress().toString());

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
