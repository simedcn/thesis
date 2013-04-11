package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.StatRequest;

/**
 * Transfer order: int type; byte retry;
 * 
 * @author: luochen
 */
public class StatRequestEncoder implements IProtocolEncoder<StatRequest> {

	@Override
	public void encode(IoSession session, StatRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
	}
}
