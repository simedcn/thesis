package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.DeleteRequest;
import com.ebay.kvstore.protocol.response.DeleteResponse;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.server.data.storage.InvalidKeyException;

public class DeleteRequestHandler extends DataServerHandler<DeleteRequest> {

	@Override
	public void handle(DataServerContext context, DeleteRequest protocol) {
		IProtocol response = null;
		IoSession session = context.getSession();
		byte[] key = protocol.getKey();
		try {
			IStoreEngine engine = context.getEngine();
			engine.delete(protocol.getKey());
			response = new DeleteResponse(ProtocolCode.Success, key);
		} catch (InvalidKeyException e) {
			response = new DeleteResponse(ProtocolCode.InvalidKey, key);
		}
		session.write(response);
	}

}
