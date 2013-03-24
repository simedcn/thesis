package com.ebay.kvstore.server.data.storage.fs;

import java.io.DataOutput;
import java.io.IOException;

public interface IBlockOutputStream extends DataOutput {
	public void close() throws IOException;

	public int getBlockAvailable();

	public int getBlockPos();

	public int getCurrentBlock();

	public int getPos();
}
