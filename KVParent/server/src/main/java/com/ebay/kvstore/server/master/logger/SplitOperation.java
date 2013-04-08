package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class SplitOperation implements IOperation {

	protected int regionId;

	protected int newRegionId;

	protected byte[] oldKeyEnd;

	protected Address addr;

	public SplitOperation() {
		this(0, 0, null, null);
	}

	public SplitOperation(int regionId, int newRegionId, Address addr, byte[] oldKeyEnd) {
		this.regionId = regionId;
		this.newRegionId = newRegionId;
		this.addr = addr;
		this.oldKeyEnd = oldKeyEnd;
	}

	@Override
	public Address getAddr() {
		return addr;
	}

	public int getNewRegionId() {
		return newRegionId;
	}

	public byte[] getOldKeyEnd() {
		return oldKeyEnd;
	}

	@Override
	public int getRegionId() {
		return regionId;
	}

	@Override
	public byte getType() {
		return Split;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		this.regionId = in.readInt();
		this.newRegionId = in.readInt();
		int length = in.readInt();
		this.oldKeyEnd = new byte[length];
		in.read(oldKeyEnd);
		this.addr = Address.parse(in.readUTF());
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(regionId);
		out.writeInt(newRegionId);
		out.writeInt(oldKeyEnd.length);
		out.write(oldKeyEnd);
		out.writeUTF(addr.toString());

	}

}
