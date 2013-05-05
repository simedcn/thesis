package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.GetRequest;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key
 * 
 */
public class GetRequestEncoder implements IProtocolEncoder<GetRequest> {

	@Override
	public void encode(IoSession session, GetRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
		buffer.put((byte) (request.isRetry() ? 1 : 0));
		buffer.putInt(request.getKey().length);
		buffer.put(request.getKey());
	}

}
