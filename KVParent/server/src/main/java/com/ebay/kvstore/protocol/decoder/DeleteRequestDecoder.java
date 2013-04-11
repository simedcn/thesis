package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.request.DeleteRequest;

public class DeleteRequestDecoder implements IProtocolDecoder<DeleteRequest> {

	@Override
	public DeleteRequest decode(IoSession session, IoBuffer in) {
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		DeleteRequest request = new DeleteRequest(key, retry);
		return request;
	}

}
