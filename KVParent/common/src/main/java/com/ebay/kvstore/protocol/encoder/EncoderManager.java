package com.ebay.kvstore.protocol.encoder;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolEncoder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EncoderManager implements ProtocolEncoder {

	private Map<Integer, IProtocolEncoder> encoders;

	private IProtocolEncoder defaultEncoder;

	public EncoderManager(IProtocolEncoder defaultEncoder) {
		encoders = new HashMap<>();
		this.defaultEncoder = defaultEncoder;
	}

	public void addEncoder(int type, IProtocolEncoder encoder) {
		encoders.put(type, encoder);
	}

	@Override
	public void dispose(IoSession session) throws Exception {
		encoders.clear();
	}

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		if (!(message instanceof IProtocol)) {
			throw new IllegalArgumentException("The message:" + message + " is not supported");
		}
		IProtocol protocol = (IProtocol) message;
		int type = protocol.getType();
		IProtocolEncoder encoder = encoders.get(type);
		if (encoder == null) {
			encoder = defaultEncoder;
		}
		IoBuffer buffer = IoBuffer.allocate(64);
		buffer.setAutoExpand(true);
		// leave space for message length
		buffer.skip(4);
		encoder.encode(session, protocol, buffer);
		// fill the length header
		int oldPos = buffer.position();
		buffer.position(0);
		buffer.putInt(oldPos - 4);
		buffer.position(oldPos);
		buffer.flip();
		out.write(buffer);
	}

	public IProtocolEncoder getDefaultEncoder() {
		return defaultEncoder;
	}

	public IProtocolEncoder removeEncoder(int type) {
		return encoders.remove(type);
	}

	public void setDefaultEncoder(IProtocolEncoder defaultEncoder) {
		this.defaultEncoder = defaultEncoder;
	}

}
