package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.StatResponse;
import com.ebay.kvstore.structure.DataServerStruct;

public class StatResponseDecoder implements IProtocolDecoder<StatResponse> {

	@Override
	public StatResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		int size = in.getInt();
		DataServerStruct[] dataServers = null;
		if (size > 0) {
			dataServers = new DataServerStruct[size];
			for (int i = 0; i < size; i++) {
				dataServers[i] = DecoderUtil.decodeDataServer(in);
			}
		}
		StatResponse response = new StatResponse(retCode, dataServers);
		return response;
	}

}
