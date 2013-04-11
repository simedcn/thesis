package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.response.IncrResponse;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key;int
 * incremental; int value
 * 
 */
public class IncrResponseEncoder implements IProtocolEncoder<IncrResponse> {

	@Override
	public void encode(IoSession session, IncrResponse response, IoBuffer buffer) {
		buffer.putInt(response.getType());
		buffer.putInt(response.getRetCode());
		buffer.put((byte) (response.isRetry() ? 1 : 0));
		buffer.putInt(response.getKey().length);
		buffer.put(response.getKey());
		buffer.putInt(response.getIncremental());
		buffer.putInt(response.getValue());
	}

}
