package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.serialization.ObjectSerializationEncoder;

import com.ebay.kvstore.protocol.IProtocol;

public class DefaultProtocolEncoder extends ObjectSerializationEncoder {

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		IProtocol protocol = (IProtocol) message;
		IoBuffer buf = IoBuffer.allocate(64);
		buf.setAutoExpand(true);
		buf.putInt(protocol.getType());
		buf.putObject(protocol);
		buf.flip();
		out.write(buf);
	}

}
