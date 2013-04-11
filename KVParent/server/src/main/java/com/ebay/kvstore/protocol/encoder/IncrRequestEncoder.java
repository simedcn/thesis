package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.IncrRequest;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key;int
 * incremental; int initValue
 * 
 */
public class IncrRequestEncoder implements IProtocolEncoder<IncrRequest> {

	@Override
	public void encode(IoSession session, IncrRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
		buffer.put((byte) (request.isRetry() ? 1 : 0));
		buffer.putInt(request.getKey().length);
		buffer.put(request.getKey());
		buffer.putInt(request.getIncremental());
		buffer.putInt(request.getInitValue());
	}
}
