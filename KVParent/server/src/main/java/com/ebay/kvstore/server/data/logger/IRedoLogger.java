package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

public interface IRedoLogger {
	public void write(IMutation mutation);

	public void close();

	public void flush() throws IOException;

	public void append(String file) throws IOException;

	public void renameTo(String newLog) throws IOException;

	public String getFile();
}
