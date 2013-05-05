package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.request.RegionTableRequest;

public class RegionTableRequestDecoder implements IProtocolDecoder<RegionTableRequest> {

	@Override
	public RegionTableRequest decode(IoSession session, IoBuffer in) {
		return new RegionTableRequest();
	}

}
