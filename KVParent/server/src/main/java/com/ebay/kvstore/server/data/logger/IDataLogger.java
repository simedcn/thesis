package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILogger;

public interface IDataLogger extends ILogger {
	public void append(String file) throws IOException;

	public void write(IMutation mutation);

}
