package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.structure.RegionTable;

/**
 * Transfer order: int type; int retCode; RegionTable table;
 * 
 * @see EncoderUtil#encodeRegionTable(RegionTable, IoBuffer)
 * @author luochen
 * 
 */
public class RegionTableResponseEncoder implements IProtocolEncoder<RegionTableResponse> {

	@Override
	public void encode(IoSession session, RegionTableResponse response, IoBuffer buffer) {
		buffer.putInt(response.getType());
		buffer.putInt(response.getRetCode());
		RegionTable table = response.getTable();
		EncoderUtil.encodeRegionTable(table, buffer);
	}
}
