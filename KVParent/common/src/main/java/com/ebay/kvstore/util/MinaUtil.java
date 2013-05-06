package com.ebay.kvstore.util;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import com.ebay.kvstore.protocol.KVProtocolCodecFactory;

public class MinaUtil {

	public static IoAcceptor getDefaultAcceptor() {
		IoAcceptor acceptor = new NioSocketAcceptor();
		//acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new KVProtocolCodecFactory()));
		acceptor.getFilterChain().addLast("executor", new ExecutorFilter());
		return acceptor;
	}

	public static IoConnector getDefaultConnector() {
		IoConnector connector = new NioSocketConnector();
		connector.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new KVProtocolCodecFactory()));
		//connector.getFilterChain().addLast("logger", new LoggingFilter());
		connector.getFilterChain().addLast("exceutor", new ExecutorFilter());
		return connector;
	}
}
