package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;

public interface LoggerInputStream {
	public void close() throws IOException;

	public int readInt() throws IOException;

	public byte read() throws IOException;

	public int read(byte[] b) throws IOException;

}
