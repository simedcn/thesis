package com.ebay.kvstore.protocol;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

public interface IProtocolEncoder<T extends IProtocol> {
	public void encode(IoSession session, T message, IoBuffer buffer);

}
