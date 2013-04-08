package com.ebay.kvstore.protocol.decoder;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class DecoderManager extends CumulativeProtocolDecoder {

	private Map<Integer, ProtocolDecoder> decoders;
	private CumulativeProtocolDecoder defaultDecoder;

	public DecoderManager(CumulativeProtocolDecoder defaultDecoder) {
		this.decoders = new HashMap<Integer, ProtocolDecoder>();
		this.defaultDecoder = defaultDecoder;
	}

	public ProtocolDecoder getDefaultDecoder() {
		return defaultDecoder;
	}

	public void setDefaultDecoder(CumulativeProtocolDecoder defaultDecoder) {
		this.defaultDecoder = defaultDecoder;
	}

	public void addDecoder(int type, ProtocolDecoder decoder) {
		decoders.put(type, decoder);
	}

	public ProtocolDecoder removeDecoder(int type) {
		return decoders.remove(type);
	}

	@Override
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
			throws Exception {
		int type = in.getInt();
		ProtocolDecoder decoder = decoders.get(type);
		if (decoder == null) {
			decoder = defaultDecoder;
		}
		decoder.decode(session, in, out);
		return true;
	}
}
