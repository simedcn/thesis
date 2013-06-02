package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.server.logger.ILoggerInputStream;
import com.ebay.kvstore.server.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class SplitOperation extends BaseOperation {

	protected int newRegionId;

	protected byte[] oldKeyEnd;

	protected byte[] newKeyEnd;

	public SplitOperation() {
		this(0, 0, null, null, null);
	}

	public SplitOperation(int regionId, int newRegionId, Address addr, byte[] oldKeyEnd,
			byte[] newKeyEnd) {
		super(regionId, addr);
		this.newRegionId = newRegionId;
		this.oldKeyEnd = oldKeyEnd;
		this.newKeyEnd = newKeyEnd;
	}

	public byte[] getNewKeyEnd() {
		return newKeyEnd;
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
		length = in.readInt();
		if (length == 0) {
			this.newKeyEnd = null;
		} else {
			this.newKeyEnd = new byte[length];
			in.read(newKeyEnd);
		}

	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		super.writeToExternal(out);
		out.writeInt(regionId);
		out.writeInt(newRegionId);
		out.writeInt(oldKeyEnd.length);
		out.write(oldKeyEnd);
		if (newKeyEnd == null) {
			out.writeInt(0);
		} else {
			out.writeInt(newKeyEnd.length);
			out.write(newKeyEnd);
		}

	}

}
