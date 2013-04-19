package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;

public interface IBlockInputStream {

	public void close() throws IOException;

	public int getBlockAvailable();

	public int getBlockPos();

	public int getCurrentBlock();

	public int getPos();

	public byte readByte() throws IOException;

	public void readFully(byte[] b) throws IOException;

	public void readFully(byte[] b, int off, int len) throws IOException;

	public int readInt() throws IOException;

	public long readLong() throws IOException;

	public int skipBytes(int n) throws IOException;
}
