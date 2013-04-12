package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class SplitOperation extends BaseOperation {

	protected int newRegionId;

	protected byte[] oldKeyEnd;

	public SplitOperation() {
		this(0, 0, null, null);
	}

	public SplitOperation(int regionId, int newRegionId, Address addr, byte[] oldKeyEnd) {
		super(regionId, addr);
		this.newRegionId = newRegionId;
		this.oldKeyEnd = oldKeyEnd;
	}

	public int getNewRegionId() {
		return newRegionId;
	}

	public byte[] getOldKeyEnd() {
		return oldKeyEnd;
	}

	@Override
	public byte getType() {
		return Split;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		super.readFromExternal(in);
		this.newRegionId = in.readInt();
		int length = in.readInt();
		this.oldKeyEnd = new byte[length];
		in.read(oldKeyEnd);
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		super.writeToExternal(out);
		out.writeInt(regionId);
		out.writeInt(newRegionId);
		out.writeInt(oldKeyEnd.length);
		out.write(oldKeyEnd);
	}

}
