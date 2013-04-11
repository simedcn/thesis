package com.ebay.kvstore.server.data;

import java.io.IOException;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.MinaUtil;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.server.data.handler.DataServerJoinResponseHandler;
import com.ebay.kvstore.server.data.handler.LoadRegionRequestHandler;
import com.ebay.kvstore.server.data.handler.SplitRegionRequestHandler;
import com.ebay.kvstore.server.data.handler.UnloadRegionRequestHandler;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Address;

public class DataClient {
	private Address masterAddr;

	private int connectTimeout;// in ms
	private int sessionTimeout;// in s
	private IoSession session;
	private ProtocolDispatcher dispatcher;
	private IConfiguration conf;
	private IStoreEngine engine;

	public DataClient(Address masterAddr, IConfiguration conf, IStoreEngine engine) {
		this.conf = conf;
		if (masterAddr != null) {
			this.masterAddr = masterAddr;
		} else {
			this.masterAddr = Address.parse(this.conf.get(IConfigurationKey.Master_Addr));
		}
		this.connectTimeout = conf.getInt(IConfigurationKey.Dataserver_Master_Connect_Timeout);
		this.sessionTimeout = conf.getInt(IConfigurationKey.Dataserver_Master_Session_Timeout);
		this.dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(IProtocolType.Load_Region_Req, new LoadRegionRequestHandler());
		dispatcher.registerHandler(IProtocolType.Unload_Region_Req,
				new UnloadRegionRequestHandler());
		dispatcher.registerHandler(IProtocolType.Split_Region_Req, new SplitRegionRequestHandler());
		dispatcher.registerHandler(IProtocolType.DataServer_Join_Resp,
				new DataServerJoinResponseHandler());

		dispatcher.registerHandler(IProtocolType.Heart_Beart_Resp, null);
		this.engine = engine;
	}

	public DataClient(IConfiguration conf, IStoreEngine engine) {
		this(null, conf, engine);
	}

	public void close() throws IOException {
		session.close(false);
		session = null;
	}

	public IoSession connect() throws IOException {
		if (session == null) {
			IoConnector connector = MinaUtil.getDefaultConnector();
			connector.setConnectTimeoutMillis(connectTimeout);
			connector.setHandler(new DataClientHandler());
			connector.getSessionConfig().setUseReadOperation(true);
			connector.getSessionConfig().setBothIdleTime(sessionTimeout);
			ConnectFuture future = connector.connect(masterAddr.toInetSocketAddress());
			future.awaitUninterruptibly();
			session = future.getSession();
		}
		return session;
	}

	public void reconnect() {
		this.session = null;
	}

	public void setMasterAddr(Address masterAddr) {
		this.masterAddr = masterAddr;
	}

	class DataClientHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(DataClientHandler.class);

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
		public void messageSent(IoSession session, Object message) throws Exception {

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
			System.out.println("Session opened " + session.getRemoteAddress().toString());
		}
	}
}
