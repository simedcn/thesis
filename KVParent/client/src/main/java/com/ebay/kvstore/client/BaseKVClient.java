package com.ebay.kvstore.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.client.async.AsyncClientContext;
import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.KVProtocolCodecFactory;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.RegionTable;

public abstract class BaseKVClient implements IKVClient {

	private static Logger logger = LoggerFactory.getLogger(BaseKVClient.class);

	protected ClientOption option;

	protected Map<Address, IoSession> connections;

	protected RegionTable table;

	protected IoHandler ioHandler;

	protected ProtocolDispatcher dispatcher;

	protected Address activeMaster;

	public BaseKVClient(ClientOption option) {
		this.option = option;
		this.connections = new HashMap<>();
		this.ioHandler = new ClientIoHandler(this);
		this.dispatcher = new ProtocolDispatcher();
	}

	@Override
	public synchronized void close() {
		Collection<IoSession> sessions = connections.values();
		for (IoSession session : sessions) {
			session.close(true);
		}
		sessions.clear();

	}

	@Override
	public ClientOption getClientOption() {
		return option;
	}

	@Override
	public void setRegionTable(RegionTable table) {
		this.table = table;
	}

	protected void checkKey(byte[] key) {
		if (key == null) {
			throw new NullPointerException("null key is not allowed");
		}
	}

	protected IoSession getConnection(Address addr) {
		IoSession session = connections.get(addr);
		if (session == null) {
			IoConnector connector = new NioSocketConnector();
			connector.setConnectTimeoutMillis(option.getConnectionTimeout());
			connector.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new KVProtocolCodecFactory()));
			connector.getFilterChain().addLast("logger", new LoggingFilter());
			connector.getFilterChain().addLast("exceutor", new ExecutorFilter());
			connector.setHandler(ioHandler);
			connector.getSessionConfig().setBothIdleTime(option.getSessionTimeout());
			connector.getSessionConfig().setUseReadOperation(option.isSync());
			ConnectFuture future = connector.connect(addr.toInetSocketAddress());
			future.awaitUninterruptibly();
			session = future.getSession();
			connections.put(addr, session);
		}
		return session;
	}

	protected IoSession getConnection(byte[] key) throws KVException {
		if (table == null) {
			updateRegionTable();
		}
		Address addr = table.getKeyAddr(key);
		if (addr == null) {
			// fail to get key for region.
			updateRegionTable();
			addr = table.getKeyAddr(key);
		}
		if (addr == null) {
			throw new InvalidKeyException("Fail to get region for key:" + Arrays.toString(key));
		}
		IoSession session = null;
		try {
			session = getConnection(addr);
		} catch (Exception e) {
			logger.error("Fail to get connection to" + addr, e);
			updateRegionTable();
			addr = table.getKeyAddr(key);
			session = getConnection(addr);
		}
		return session;
	}

	protected IoSession getMasterConnection() throws KVException {
		IoSession connection = null;
		if (activeMaster != null) {
			try {
				connection = getConnection(activeMaster);
			} catch (RuntimeException e) {
				logger.error("Fail to connect to master " + activeMaster, e);
			}
		}
		if (connection == null) {
			Collection<Address> masters = option.getMasterAddrs();
			for (Address master : masters) {
				try {
					connection = getConnection(master);
					if (connection != null) {
						activeMaster = master;
						return connection;
					}
				} catch (RuntimeException e) {
					logger.error("Fail to connect to master " + activeMaster + ", try next");
				}
			}
		}
		if (connection != null) {
			return connection;
		} else {
			throw new KVException(
					"No active master found in cluster, please check the configuration.");
		}
	}

	protected void removeConnection(IoSession session) {
		Iterator<Entry<Address, IoSession>> it = connections.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Address, IoSession> e = it.next();
			if (e.getValue().equals(session)) {
				it.remove();
				return;
			}
		}
	}

	protected class ClientIoHandler implements IoHandler {

		private IKVClient client = null;

		public ClientIoHandler(IKVClient client) {
			this.client = client;
		}

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		}

		@Override
		public void messageReceived(IoSession session, Object message) throws Exception {
			logger.info("Message received from " + session.getRemoteAddress().toString() + " "
					+ message);
			try {
				IContext context = new AsyncClientContext(client, session);
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
			removeConnection(session);
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
