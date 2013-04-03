package com.ebay.kvstore.server.data.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.DeleteRequest;
import com.ebay.kvstore.protocol.response.DeleteResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;

public class DeleteRequestHandler extends DataServerHandler<DeleteRequest> {

	@Override
	public void handle(DataServerContext context, DeleteRequest protocol) {
		IProtocol response = null;
		IoSession session = context.getSession();
		byte[] key = protocol.getKey();
		boolean retry = protocol.isRetry();
		try {
			IStoreEngine engine = context.getEngine();
			engine.delete(protocol.getKey());
			response = new DeleteResponse(ProtocolCode.Success, key, retry);
		} catch (InvalidKeyException e) {
			response = new DeleteResponse(ProtocolCode.InvalidKey, key, retry);
		}
		session.write(response);
	}

}
