package com.ebay.kvstore.logger;

import java.io.IOException;

public interface ILogger {
	public void close();

	public String getFile();

	public void renameTo(String newLog) throws IOException;

}
