package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.response.SetResponse;

public class SetResponseDecoder implements IProtocolDecoder<SetResponse> {

	@Override
	public SetResponse decode(IoSession session, IoBuffer in) {
		int retCode = in.getInt();
		byte b = in.get();
		boolean retry = (b != 0) ? true : false;
		int ttl = in.getInt();
		int length = in.getInt();
		byte[] key = new byte[length];
		in.get(key);
		length = in.getInt();
		byte[] value = null;
		if (length > 0) {
			value = new byte[length];
			in.get(value);
		}
		SetResponse response = new SetResponse(retCode, key, value, ttl, retry);
		return response;
	}

}
