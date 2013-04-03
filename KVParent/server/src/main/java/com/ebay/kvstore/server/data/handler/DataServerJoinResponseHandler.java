package com.ebay.kvstore.server.data.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.DataServerJoinResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;

public class DataServerJoinResponseHandler extends DataServerHandler<DataServerJoinResponse> {

	@Override
	public void handle(DataServerContext context, DataServerJoinResponse protocol) {
		IoSession session = context.getSession();
		IStoreEngine engine = context.getEngine();

		synchronized (engine) {
			if (protocol.getRetCode() == ProtocolCode.Success) {
				session.setAttribute("success", true);
				session.setAttribute("code", protocol.getRetCode());
			}
			engine.notifyAll();
		}

	}

}
