package com.ebay.kvstore.protocol.decoder;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolDecoder;

@SuppressWarnings("rawtypes")
public class DecoderManager extends CumulativeProtocolDecoder {

	private Map<Integer, IProtocolDecoder> decoders;
	private IProtocolDecoder defaultDecoder;

	public DecoderManager(IProtocolDecoder defaultDecoder) {
		this.decoders = new HashMap<Integer, IProtocolDecoder>();
		this.defaultDecoder = defaultDecoder;
	}

	public IProtocolDecoder getDefaultDecoder() {
		return defaultDecoder;
	}

	public void setDefaultDecoder(IProtocolDecoder defaultDecoder) {
		this.defaultDecoder = defaultDecoder;
	}

	public void addDecoder(int type, IProtocolDecoder decoder) {
		decoders.put(type, decoder);
	}

	public IProtocolDecoder removeDecoder(int type) {
		return decoders.remove(type);
	}

	@Override
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
			throws Exception {
		if (!in.prefixedDataAvailable(4, Integer.MAX_VALUE)) {
			return false;
		}
		int length = in.getInt();
		if (length < 4) {
			throw new IllegalArgumentException(
					"Message length should be larger than 4, while real length is " + length);
		}
		int type = in.getInt();
		IProtocolDecoder decoder = decoders.get(type);
		if (decoder == null) {
			decoder = defaultDecoder;
		}
		IProtocol protocol = decoder.decode(session, in);
		out.write(protocol);
		return true;
	}
}
