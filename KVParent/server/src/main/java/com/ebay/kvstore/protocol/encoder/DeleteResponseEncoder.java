package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.response.DeleteResponse;

/**
 * Transfer order: int type; int retCode; byte retry; int key.length; byte[]
 * key;
 * 
 * @author luochen
 * 
 */
public class DeleteResponseEncoder extends ProtocolEncoderAdapter {

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		DeleteResponse response = (DeleteResponse) message;
		IoBuffer buf = IoBuffer.allocate(64);
		buf.setAutoExpand(true);
		buf.putInt(response.getType());
		buf.putInt(response.getRetCode());
		buf.put((byte) (response.isRetry() ? 1 : 0));
		buf.putInt(response.getKey().length);
		buf.put(response.getKey());
		buf.flip();
		out.write(buf);
	}
}
