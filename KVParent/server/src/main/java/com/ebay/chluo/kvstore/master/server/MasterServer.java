package com.ebay.chluo.kvstore.master.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.IServer;
import com.ebay.chluo.kvstore.conf.IConfiguration;
import com.ebay.chluo.kvstore.conf.ServerConstants;
import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.chluo.kvstore.zookeeper.MasterWatcher;

public class MasterServer implements IServer {
	private static Logger logger = LoggerFactory.getLogger(MasterServer.class);
	private int port;
	private String ip;
	private IoAcceptor acceptor;

	private int zkPort;
	private String zkIp;
	private ZooKeeper zooKeeper;

	private int sessionTimeOut;
	private ProtocolDispatcher dispatcher;

	public static void main(String[] args) {
		try {
			MasterServer server = new MasterServer(null);
			server.run();
		} catch (Exception e) {
			logger.error("Fail to start master server", e);
		}
	}

	public MasterServer(IConfiguration conf) throws IOException {
		port = 1111;
		ip = "127.0.0.1";

		zkPort = 2181;
		zkIp = "127.0.0.1";

		sessionTimeOut = 10 * 1000;

		dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(ProtocolType.Heart_Beart_Req, null);
		dispatcher.registerHandler(ProtocolType.Region_Table_Req, null);

		dispatcher.registerHandler(ProtocolType.Load_Region_Resp, null);
		dispatcher.registerHandler(ProtocolType.Unload_Region_Resp, null);
		dispatcher.registerHandler(ProtocolType.Split_Region_Resp, null);

	}

	private void initServer() throws IOException {
		acceptor = new NioSocketAcceptor();
		// filter chain
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		// filter handler
		acceptor.setHandler(new MasterServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(new InetSocketAddress(ip, port));
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkIp + ":" + zkPort, sessionTimeOut, new MasterWatcher());
		// create the path recursively...
		if (zooKeeper.exists(ServerConstants.ZooKeeper_Master_Dir, false) == null) {
			zooKeeper.create(ServerConstants.ZooKeeper_Base, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			zooKeeper.create(ServerConstants.ZooKeeper_Master_Dir, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		zooKeeper.create(ServerConstants.ZooKeeper_Master_Dir_Path, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		zooKeeper.setData(ServerConstants.ZooKeeper_Master_Dir, (ip + ":" + port).getBytes(), -1);

	}

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
	}

	public void run() throws Exception {
		initServer();
		initZookeeper();
	}

	private class MasterServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(MasterServerHandler.class);

		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
			logger.error("Error occured with " + session.getRemoteAddress().toString(), error);
		}

		public void messageReceived(IoSession session, Object obj) throws Exception {
			/*
			 * if (obj instanceof SimpleRequest) {
			 * System.out.println("Message received from " +
			 * session.getRemoteAddress().toString() + " " + obj);
			 * session.write(new SimpleResponse("Hi, I'm Master")); } else {
			 * System.err.println("Malformat message " + obj); }
			 */
		}

		public void messageSent(IoSession session, Object arg1) throws Exception {

		}

		public void sessionClosed(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session closed " + session.getRemoteAddress().toString());
		}

		public void sessionCreated(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session created " + session.getRemoteAddress().toString());

		}

		public void sessionIdle(IoSession session, IdleStatus arg1) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session idle " + session.getRemoteAddress().toString());

		}

		public void sessionOpened(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session opened " + session.getRemoteAddress().toString());

		}

	}

}
