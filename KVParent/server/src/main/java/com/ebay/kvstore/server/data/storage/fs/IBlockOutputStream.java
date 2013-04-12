package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;

public interface IBlockOutputStream {
	public void close() throws IOException;

	public int getBlockAvailable();

	public int getBlockPos();

	public int getCurrentBlock();

	public int getPos();

	public void write(byte[] b) throws IOException;

	public void writeByte(int v) throws IOException;

	public void writeInt(int v) throws IOException;
}
