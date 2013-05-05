package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.request.RegionTableRequest;

/**
 * Transfer order: int type;
 * 
 */
public class RegionTableRequestEncoder implements IProtocolEncoder<RegionTableRequest> {

	@Override
	public void encode(IoSession session, RegionTableRequest request, IoBuffer buffer) {
		buffer.putInt(request.getType());
	}
}
