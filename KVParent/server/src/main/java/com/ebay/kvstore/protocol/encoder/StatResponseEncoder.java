package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.response.StatResponse;
import com.ebay.kvstore.structure.DataServerStruct;

/**
 * Transfer order:int type; int retCode; int dataServers.length;
 * (DataServerStruct dataServer){length};
 * 
 * @author luochen
 * @see EncoderUtil#encodeDataServerStruct(DataServerStruct, IoBuffer)
 */
public class StatResponseEncoder implements IProtocolEncoder<StatResponse> {

	@Override
	public void encode(IoSession session, StatResponse response, IoBuffer buffer) {
		buffer.putInt(response.getType());
		buffer.putInt(response.getRetCode());
		DataServerStruct[] dataServers = response.getServers();
		if (dataServers == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(dataServers.length);
			for (DataServerStruct dataServer : dataServers) {
				EncoderUtil.encodeDataServerStruct(dataServer, buffer);
			}
		}

	}

}
