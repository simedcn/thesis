package com.ebay.kvstore.server.data.storage.fs;

import java.io.DataInput;
import java.io.IOException;

public interface IBlockInputStream extends DataInput {

	public int getCurrentBlock();

	public int getPos();

	public int getBlockPos();

	public int getBlockAvailable();

	public void close() throws IOException;
}
