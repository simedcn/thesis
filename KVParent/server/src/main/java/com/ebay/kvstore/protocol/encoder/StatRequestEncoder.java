package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.request.RegionTableRequest;

/**
 * Transfer order: int type; byte retry;
 * 
 * @author: luochen
 */
public class StatRequestEncoder extends ProtocolEncoderAdapter {

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		RegionTableRequest request = (RegionTableRequest) message;
		IoBuffer buf = IoBuffer.allocate(64);
		buf.setAutoExpand(true);
		buf.putInt(request.getType());
		buf.put((byte) (request.isRetry() ? 1 : 0));
		buf.flip();
		out.write(buf);
	}

}
