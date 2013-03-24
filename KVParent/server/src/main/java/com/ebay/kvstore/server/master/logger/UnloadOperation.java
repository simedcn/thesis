package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;

public class UnloadOperation implements IOperation {
	protected int regionId;

	protected Address addr;

	public UnloadOperation() {
		this(0, null);
	}

	public UnloadOperation(int regionId, Address addr) {
		this.regionId = regionId;
		this.addr = addr;
	}

	@Override
	public Address getAddr() {
		return addr;
	}

	@Override
	public int getRegionId() {
		return regionId;
	}

	@Override
	public byte getType() {
		return Unload;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		this.regionId = in.readInt();
		this.addr = Address.parse(in.readUTF());
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.writeInt(getType());
		out.writeInt(regionId);
		out.writeUTF(addr.toString());
	}

}
