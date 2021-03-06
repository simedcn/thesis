package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocolEncoder;
import com.ebay.kvstore.protocol.response.GetResponse;

/**
 * Transfer order: int type; int retCode; byte retry; int key.length; byte[]
 * key;int value.length; byte[] value;
 * 
 * @author luochen
 * 
 */
public class GetResponseEncoder implements IProtocolEncoder<GetResponse> {

	@Override
	public void encode(IoSession session, GetResponse response, IoBuffer buffer) {
		buffer.putInt(response.getType());
		buffer.putInt(response.getRetCode());
		buffer.put((byte) (response.isRetry() ? 1 : 0));
		buffer.putInt(response.getTtl());
		buffer.putInt(response.getKey().length);
		buffer.put(response.getKey());
		if (response.getValue() == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(response.getValue().length);
			buffer.put(response.getValue());
		}

	}

}
