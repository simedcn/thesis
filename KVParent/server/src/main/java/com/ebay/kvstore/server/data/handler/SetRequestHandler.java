package com.ebay.kvstore.server.data.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.SetRequest;
import com.ebay.kvstore.protocol.response.SetResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;

public class SetRequestHandler extends DataServerHandler<SetRequest> {

	@Override
	public void handle(DataServerContext context, SetRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		byte[] key = protocol.getKey();
		byte[] value = protocol.getValue();
		int ttl = protocol.getTtl();
		IProtocol response = null;
		boolean retry = protocol.isRetry();
		try {
			engine.set(key, value, ttl);
			response = new SetResponse(ProtocolCode.Success, key, value, ttl, retry);
		} catch (InvalidKeyException e) {
			response = new SetResponse(ProtocolCode.Invalid_Key, key, value, ttl, retry);
		}
		session.write(response);
	}
}
