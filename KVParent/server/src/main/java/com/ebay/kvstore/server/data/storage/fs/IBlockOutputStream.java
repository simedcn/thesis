package com.ebay.kvstore.server.data.storage.fs;

import java.io.DataOutput;
import java.io.IOException;

public interface IBlockOutputStream extends DataOutput {
	public int getCurrentBlock();

	public int getPos();

	public int getBlockPos();

	public int getBlockAvailable();

	public void close() throws IOException;
}
