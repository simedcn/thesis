package com.ebay.kvstore.protocol.encoder;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.IProtocol;

public class EncoderManager implements ProtocolEncoder {

	private Map<Integer, ProtocolEncoder> encoders;

	private ProtocolEncoder defaultEncoder;

	public EncoderManager(ProtocolEncoder defaultEncoder) {
		encoders = new HashMap<>();
		this.defaultEncoder = defaultEncoder;
	}

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		if (!(message instanceof IProtocol)) {
			throw new IllegalArgumentException("The message:" + message + " is not supported");
		}
		IProtocol protocol = (IProtocol) message;
		int type = protocol.getType();
		ProtocolEncoder encoder = encoders.get(type);
		if (encoder == null) {
			encoder = defaultEncoder;
		}
		encoder.encode(session, message, out);
	}

	public void addEncoder(int type, ProtocolEncoder encoder) {
		encoders.put(type, encoder);
	}

	public ProtocolEncoder removeEncoder(int type) {
		return encoders.remove(type);
	}

	public ProtocolEncoder getDefaultEncoder() {
		return defaultEncoder;
	}

	public void setDefaultEncoder(ProtocolEncoder defaultEncoder) {
		this.defaultEncoder = defaultEncoder;
	}

	@Override
	public void dispose(IoSession session) throws Exception {
		encoders.clear();
	}

}
