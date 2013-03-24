package com.ebay.kvstore.logger;

import java.io.IOException;

public interface ILogger {
	public void close();

	public void flush() throws IOException;

	public String getFile();

	public void renameTo(String newLog) throws IOException;

}
