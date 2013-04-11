package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolDecoder;
import com.ebay.kvstore.protocol.request.SetRequest;

public class SetRequestDecoder implements IProtocolDecoder<SetRequest> {

	@Override
	public SetRequest decode(IoSession session, IoBuffer in) {
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
		SetRequest request = new SetRequest(key, value, retry);
		return request;
	}

}
