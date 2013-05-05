package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolDecoder;

public class DefaultProtocolDecoder implements IProtocolDecoder<IProtocol> {

	private ClassLoader classLoader;

	public DefaultProtocolDecoder(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public IProtocol decode(IoSession session, IoBuffer in) throws Exception {
		return (IProtocol) in.getObject(classLoader);
	}

}
