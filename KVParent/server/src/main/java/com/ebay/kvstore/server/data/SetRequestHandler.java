package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.SetRequest;
import com.ebay.kvstore.protocol.response.SetResponse;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.server.data.storage.InvalidKeyException;

public class SetRequestHandler extends DataServerHandler<SetRequest> {

	@Override
	public void handle(DataServerContext context, SetRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		byte[] key = protocol.getKey();
		byte[] value = protocol.getValue();
		IProtocol response = null;
		try {
			engine.set(key, value);
			response = new SetResponse(ProtocolCode.Success, key, value);
		} catch (InvalidKeyException e) {
			response = new SetResponse(ProtocolCode.InvalidKey, key, value);
		}
		session.write(response);
	}
}
