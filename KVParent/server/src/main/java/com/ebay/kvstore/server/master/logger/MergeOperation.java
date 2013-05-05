package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.server.logger.ILoggerInputStream;
import com.ebay.kvstore.server.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public class MergeOperation extends BaseOperation {

	protected int regionId1;

	protected int regionId2;

	public MergeOperation() {
		this(0, null, 0, 0);
	}

	public MergeOperation(int regionId, Address addr, int regionId1, int regionId2) {
		super(regionId, addr);
		this.regionId1 = regionId1;
		this.regionId2 = regionId2;
	}

	public int getRegionId1() {
		return regionId1;
	}

	public int getRegionId2() {
		return regionId2;
	}

	@Override
	public byte getType() {
		return Merge;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		super.readFromExternal(in);
		this.regionId1 = in.readInt();
		this.regionId2 = in.readInt();
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		super.writeToExternal(out);
		out.writeInt(this.regionId1);
		out.writeInt(this.regionId2);
	}
}
