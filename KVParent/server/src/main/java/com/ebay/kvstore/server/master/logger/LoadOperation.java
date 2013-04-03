package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class LoadOperation implements IOperation {

	protected int regionId;

	protected Address addr;

	public LoadOperation() {
		this(0, null);
	}

	public LoadOperation(int regionId, Address addr) {
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
		return Load;
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

}
