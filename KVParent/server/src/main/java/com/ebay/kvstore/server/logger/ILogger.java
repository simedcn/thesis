package com.ebay.kvstore.server.logger;

import java.io.IOException;

public interface ILogger {
	public void append(String file) throws IOException;

	public void close();

	public String getFile();

	public void renameTo(String newLog) throws IOException;

	public void write(ILogEntry entry);

}
