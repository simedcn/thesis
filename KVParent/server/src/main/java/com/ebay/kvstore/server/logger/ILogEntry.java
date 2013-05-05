package com.ebay.kvstore.server.logger;

import java.io.IOException;

public interface ILogEntry {

	public byte getType();


	void readFromExternal(ILoggerInputStream in) throws IOException;

	void writeToExternal(ILoggerOutputStream out) throws IOException;
}
