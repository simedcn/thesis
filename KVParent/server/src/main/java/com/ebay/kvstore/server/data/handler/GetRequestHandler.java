package com.ebay.kvstore.server.data.handler;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.GetRequest;
import com.ebay.kvstore.protocol.response.GetResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.KeyValue;

public class GetRequestHandler extends DataServerHandler<GetRequest> {

	@Override
	public void handle(DataServerContext context, GetRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		IProtocol response = null;
		byte[] key = protocol.getKey();
		boolean retry = protocol.isRetry();
		try {
			KeyValue kv = engine.get(key);
			byte[] value = null;
			if (kv != null && kv.getValue() != null) {
				value = kv.getValue().getValue();
			}
			response = new GetResponse(ProtocolCode.Success, key, value, retry);
		} catch (InvalidKeyException e) {
			response = new GetResponse(ProtocolCode.Invalid_Key, key, null, retry);
		} catch (IOException e) {
			response = new GetResponse(ProtocolCode.Dataserver_Io_Error, key, null, retry);
		}
		session.write(response);
	}

}
