package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.response.DeleteResponse;

/**
 * Transfer order: int type; int retCode; byte retry; int key.length; byte[]
 * key;
 * 
 * @author luochen
 * 
 */
public class DeleteResponseEncoder implements IProtocolEncoder<DeleteResponse> {

	@Override
	public void encode(IoSession session, DeleteResponse response, IoBuffer buffer) {
		buffer.putInt(response.getType());
		buffer.putInt(response.getRetCode());
		buffer.put((byte) (response.isRetry() ? 1 : 0));
		buffer.putInt(response.getKey().length);
		buffer.put(response.getKey());
	}
}
