package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;

public interface LoggerOutputStream {

	public void close() throws IOException;

	public void writeInt(int i) throws IOException;

	public void write(byte b) throws IOException;

	public void write(byte[] b) throws IOException;
	
}
