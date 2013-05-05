package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.request.GetRequest;

public class GetRequestDecoder implements IProtocolDecoder<GetRequest> {

	@Override
	public GetRequest decode(IoSession session, IoBuffer in) {
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		GetRequest request = new GetRequest(key, retry);
		return request;
	}

}
