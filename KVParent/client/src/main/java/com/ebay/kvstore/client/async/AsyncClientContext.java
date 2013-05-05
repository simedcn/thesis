package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.protocol.context.IContext;

public class AsyncClientContext implements IContext {

	private IKVClient client;
	private IoSession session;

	public AsyncClientContext(IKVClient client, IoSession session) {
		super();
		this.client = client;
		this.session = session;
	}

	public IKVClient getClient() {
		return client;
	}

	public IoSession getSession() {
		return session;
	}

}
