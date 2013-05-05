package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolEncoder;

public class DefaultProtocolEncoder implements IProtocolEncoder<IProtocol> {

	@Override
	public void encode(IoSession session, IProtocol protocol, IoBuffer buffer) {
		buffer.putInt(protocol.getType());
		buffer.putObject(protocol);
	}

}
