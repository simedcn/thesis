package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.GetResponse;

public class GetResponseDecoder implements IProtocolDecoder<GetResponse> {

	@Override
	public GetResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		length = in.getInt();
		byte[] value = null;
		if (length > 0) {
			value = new byte[length];
			in.get(value);
		}
		GetResponse response = new GetResponse(retCode, key, value, retry);
		return response;
	}
}
