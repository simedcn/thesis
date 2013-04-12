package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class LoadOperation extends BaseOperation {

	public LoadOperation() {
		this(0, null);
	}

	public LoadOperation(int regionId, Address addr) {
		super(regionId,addr);
	}

	@Override
	public byte getType() {
		return Load;
	}


}
