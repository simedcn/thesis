package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;

public class DataServerContext implements IContext {

	private IStoreEngine engine;

	private IoSession session;

	public DataServerContext(IStoreEngine engine, IoSession session) {
		this.engine = engine;
		this.session = session;
	}

	public IStoreEngine getEngine() {
		return engine;
	}

	public IoSession getSession() {
		return session;
	}

}
