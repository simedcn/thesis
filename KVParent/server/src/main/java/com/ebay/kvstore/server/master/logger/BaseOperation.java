package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public abstract class BaseOperation implements IOperation {
	protected int regionId;

	protected Address addr;

	@Override
	public Address getAddr() {
		return addr;
	}

	@Override
	public int getRegionId() {
		return regionId;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		regionId = in.readInt();
		addr = Address.parse(in.readUTF());
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(regionId);
		out.writeUTF(addr.toString());
	}

	public BaseOperation(int regionId, Address addr) {
		super();
		this.regionId = regionId;
		this.addr = addr;
	}
}
