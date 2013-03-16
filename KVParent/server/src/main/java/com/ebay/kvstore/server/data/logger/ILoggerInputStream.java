package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

public interface ILoggerInputStream {
	public void close() throws IOException;

	public int readInt() throws IOException;

	public byte read() throws IOException;

	public int read(byte[] b) throws IOException;

}
