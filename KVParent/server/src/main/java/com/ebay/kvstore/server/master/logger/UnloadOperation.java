package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class UnloadOperation extends BaseOperation {
	protected int regionId;

	protected Address addr;

	public UnloadOperation() {
		this(0, null);
	}

	public UnloadOperation(int regionId, Address addr) {
		super(regionId, addr);
		this.regionId = regionId;
		this.addr = addr;
	}


	@Override
	public byte getType() {
		return Unload;
	}

}
