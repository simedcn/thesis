package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.structure.RegionTable;

public class RegionTableResponseDecoder implements IProtocolDecoder<RegionTableResponse> {
	@Override
	public RegionTableResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		RegionTable table = DecoderUtil.decodeRegionTable(in);
		RegionTableResponse response = new RegionTableResponse(retCode, table);
		return response;
	}

}
