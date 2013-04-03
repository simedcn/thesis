package com.ebay.kvstore;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LogLevel;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public class MinaUtil {

	public static IoAcceptor getDefaultAcceptor() {
		IoAcceptor acceptor = new NioSocketAcceptor();
		LoggingFilter logging = new LoggingFilter();
		logging.setSessionIdleLogLevel(LogLevel.DEBUG);
		acceptor.getFilterChain().addLast("logger", logging);
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		acceptor.getFilterChain().addLast("executor", new ExecutorFilter());
		return acceptor;
	}

	public static IoConnector getDefaultConnector() {
		IoConnector connector = new NioSocketConnector();
		connector.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		connector.getFilterChain().addLast("logger", new LoggingFilter());
		connector.getFilterChain().addLast("exceutor", new ExecutorFilter());
		return connector;
	}
}
