package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.IncrResponse;

public class IncrResponseDecoder implements IProtocolDecoder<IncrResponse> {

	@Override
	public IncrResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		int incremental = in.getInt();
		int value = in.getInt();
		IncrResponse response = new IncrResponse(retCode, key, incremental, value, retry);
		return response;
	}

}
