package com.ebay.kvstore.server.data;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.IServer;
import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.InvalidConfException;
import com.ebay.kvstore.conf.ServerConstants;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.protocol.request.DataServerJoinRequest;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.server.data.storage.StoreEngineFactory;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.structure.DataServerStruct;

public class DataServer implements IServer, IConfigurationKey, Watcher {
	private class DataServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(DataServerHandler.class);

		@Override
		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
			logger.error("Error occured with " + session.getRemoteAddress().toString(), error);

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
			System.out.println("Session closed " + session.getRemoteAddress().toString());
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
		}
	}

	private static Logger logger = LoggerFactory.getLogger(DataServer.class);

	public static void main(String[] args) {
		DataServer server = null;
		try {
			IConfiguration conf = ConfigurationLoader.load();
			server = new DataServer(conf);
			server.start();
		} catch (Exception e) {
			logger.error("Fail to start master server", e);
			server.shutdown();
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

	public DataServer(IConfiguration conf) throws IOException {
		this.conf = conf;
		dsAddr = Address.parse(conf.get(DataServer_Addr));
		zkAddr = Address.parse(conf.get(ZooKeeper_Addr));
		hdfsAddr = Address.parse(conf.get(HDFS_Addr));
		zkSessionTimeout = conf.getInt(ZooKeeper_Session_Timeout);
		reconnectInteval = conf.getInt(IConfigurationKey.DataServer_Reconnect_Interval);
		reconnectRetry = conf.getInt(IConfigurationKey.DataServer_Reconnect_Retry);
		weight = conf.getInt(IConfigurationKey.DataServer_Weight);

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
		if (event.getPath() != null
				&& event.getPath().equals(ServerConstants.ZooKeeper_Master_Addr)) {
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

	@Override
	public void shutdown() {
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

	@Override
	public void start() throws Exception {
		initHDFS();
		initZookeeper();
		initServer();
		initStoreEngine();
		initConnection();

		// IoSession session = client.connect();
		// session.write(new SimpleRequest("hello", "master"));
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
		struct.addRegions(engine.getRegions());
		IProtocol resquest = new DataServerJoinRequest(struct);
		for (int i = 1; i <= retry; i++) {
			try {
				logger.info("Try to connect to master server, for " + i + " times");
				byte[] data = null;
				if (zooKeeper.exists(ServerConstants.ZooKeeper_Master_Addr, false) == null
						|| (data = zooKeeper.getData(ServerConstants.ZooKeeper_Master_Addr, false,
								null)) == null) {
					Thread.sleep(reconnectInteval);
					continue;
				}
				String masterAddr = new String(data);
				client = new DataClient(Address.parse(masterAddr), conf, engine);
				session = client.connect();
				session.write(resquest);
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
			}
		}
		if (success) {
			zooKeeper.getData(ServerConstants.ZooKeeper_Master_Addr, true, null);
			heartBeater = new HeartBeater(conf, engine, session);
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
		acceptor = new NioSocketAcceptor();
		// filter chain
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
		// filter handler
		acceptor.setHandler(new DataServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(dsAddr.toInetSocketAddress());
	}

	private void initStoreEngine() throws IOException {
		String type = conf.get(IConfigurationKey.Storage_Policy);
		switch (type) {
		case "persistent":
			engine = StoreEngineFactory.getInstance().getPersistentStore(conf);
			break;
		case "memory":
			engine = StoreEngineFactory.getInstance().getMemoryStore(conf);
		default:
			throw new InvalidConfException(IConfigurationKey.Storage_Policy, "persistent|memory",
					type);
		}
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, this);
	}
}
