package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.DeleteRequest;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key
 * 
 * @author luochen
 * 
 */
public class DeleteRequestEncoder implements IProtocolEncoder<DeleteRequest> {
	@Override
	public void encode(IoSession session, DeleteRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
		buffer.put((byte) (request.isRetry() ? 1 : 0));
		buffer.putInt(request.getKey().length);
		buffer.put(request.getKey());
	}

}
