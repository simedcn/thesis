package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.DeleteResponse;

public class DeleteResponseDecoder implements IProtocolDecoder<DeleteResponse> {

	@Override
	public DeleteResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		DeleteResponse response = new DeleteResponse(retCode, key, retry);
		return response;
	}
}
