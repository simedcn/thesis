package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.request.IncrRequest;

public class IncrRequestDecoder implements IProtocolDecoder<IncrRequest> {

	@Override
	public IncrRequest decode(IoSession session, IoBuffer in) {
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		int incremental = in.getInt();
		int initValue = in.getInt();
		IncrRequest request = new IncrRequest(key, incremental, initValue, retry);
		return request;
	}

}
