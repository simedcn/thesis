package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.request.SetRequest;

/**
 * Transfer order: int type; byte retry; int key.length; byte[] key; int
 * value.length; byte[] value;
 * 
 */
public class SetRequestEncoder extends ProtocolEncoderAdapter {

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		SetRequest request = (SetRequest) message;
		IoBuffer buf = IoBuffer.allocate(64);
		buf.setAutoExpand(true);
		buf.putInt(request.getType());
		buf.put((byte) (request.isRetry() ? 1 : 0));
		buf.putInt(request.getKey().length);
		buf.put(request.getKey());
		buf.putInt(request.getValue().length);
		buf.put(request.getValue());
		buf.flip();
		out.write(buf);
	}

}
