package com.ebay.kvstore.protocol;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

public interface IProtocolDecoder<T extends IProtocol> {
	T decode(IoSession session, IoBuffer in) throws Exception;
}
