package com.ebay.kvstore.server.data.storage.fs;

import java.io.DataInput;
import java.io.IOException;

public interface IBlockInputStream extends DataInput {

	public void close() throws IOException;

	public int getBlockAvailable();

	public int getBlockPos();

	public int getCurrentBlock();

	public int getPos();
}
