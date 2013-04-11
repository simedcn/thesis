package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.SetRequest;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key; int
 * value.length; byte[] value;
 * 
 */
public class SetRequestEncoder implements IProtocolEncoder<SetRequest> {

	@Override
	public void encode(IoSession session, SetRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
		buffer.put((byte) (request.isRetry() ? 1 : 0));
		buffer.putInt(request.getKey().length);
		buffer.put(request.getKey());
		if (request.getValue() == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(request.getValue().length);
			buffer.put(request.getValue());
		}
	}

}
