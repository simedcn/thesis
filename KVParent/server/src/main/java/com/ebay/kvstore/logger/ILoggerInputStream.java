package com.ebay.kvstore.logger;

import java.io.IOException;

public interface ILoggerInputStream {
	public void close() throws IOException;

	public byte read() throws IOException;

	public int read(byte[] b) throws IOException;

	public int readInt() throws IOException;

	public String readUTF() throws IOException;

	public long readLong() throws IOException;
}
