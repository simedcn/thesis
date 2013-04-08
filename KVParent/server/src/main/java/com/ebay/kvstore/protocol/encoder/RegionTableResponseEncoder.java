package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.structure.RegionTable;

public class RegionTableResponseEncoder extends ProtocolEncoderAdapter {

	@Override
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
			throws Exception {
		RegionTableResponse response = (RegionTableResponse) message;
		IoBuffer buf = IoBuffer.allocate(64);
		buf.setAutoExpand(true);
		buf.putInt(response.getType());
		buf.putInt(response.getRetCode());
		RegionTable table = response.getTable();
		EncoderUtil.encodeRegionTable(table, buf);
		buf.flip();
		out.write(buf);
	}

}
