package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.server.master.helper.IMasterEngine;

public class MasterContext implements IContext {

	private IoSession session;

	private IMasterEngine engine;

	private IConfiguration conf;

	public MasterContext(IMasterEngine engine, IoSession session, IConfiguration conf) {
		this.engine = engine;
		this.session = session;
	}

	public IConfiguration getConf() {
		return conf;
	}

	public IMasterEngine getEngine() {
		return engine;
	}

	public IoSession getSession() {
		return session;
	}
}
