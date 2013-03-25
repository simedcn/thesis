package com.ebay.kvstore.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.client.sync.RegionTableResponseHandler;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.structure.RegionTable;

public abstract class BaseClient implements IKVClient {

	private static Logger logger = LoggerFactory.getLogger(BaseClient.class);

	protected ClientOption option;

	protected Map<Address, IoSession> connections;

	protected RegionTable table;

	protected IoHandler ioHandler;

	protected ProtocolDispatcher dispatcher;

	public BaseClient(ClientOption option) {
		this.option = option;
		this.connections = new HashMap<>();
		this.ioHandler = new ClientIoHandler();
		this.dispatcher = new ProtocolDispatcher();

		this.dispatcher.registerHandler(IProtocolType.Region_Table_Resp,
				new RegionTableResponseHandler());
	}

	public ClientOption getClientOption() {
		return option;
	}

	// TODO
	protected IoSession getConnection(Address addr) {
		IoSession session = connections.get(addr);
		if (session == null) {
			IoConnector connector = new NioSocketConnector();
			connector.setConnectTimeoutMillis(option.getConnectionTimeout());
			connector.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
			connector.getFilterChain().addLast("logger", new LoggingFilter());
			connector.getFilterChain().addLast("exceutor", new ExecutorFilter());
			connector.setHandler(ioHandler);
			connector.getSessionConfig().setUseReadOperation(true);
			ConnectFuture future = connector.connect(addr.toInetSocketAddress());
			future.awaitUninterruptibly();
			session = future.getSession();
			connections.put(addr, session);
		}
		return session;
	}

	// TODO
	protected void updateRegionTable() {

	}

	protected class ClientIoHandler implements IoHandler {

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		}

		@Override
		public void messageReceived(IoSession session, Object message) throws Exception {
			logger.info("Message received from " + session.getRemoteAddress().toString() + " "
					+ message);
			try {
				IContext context = new ClientContext();
				dispatcher.handle(message, context);
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

		}

		@Override
		public void sessionIdle(IoSession session, IdleStatus status) throws Exception {

		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {

		}

	}

}
