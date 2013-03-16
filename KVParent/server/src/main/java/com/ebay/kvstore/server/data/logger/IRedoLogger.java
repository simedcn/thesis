package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

public interface IRedoLogger {
	public void write(IMutation mutation);

	public void close();

	public void flush() throws IOException;
	
	public String getFile();
}
