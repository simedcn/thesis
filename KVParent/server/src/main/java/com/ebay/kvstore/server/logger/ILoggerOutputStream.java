package com.ebay.kvstore.server.logger;

import java.io.IOException;

public interface ILoggerOutputStream {

	public void close() throws IOException;

	public void write(byte b) throws IOException;

	public void write(byte[] b) throws IOException;

	public void writeInt(int i) throws IOException;

	public void writeLong(long expire) throws IOException;

	public void writeUTF(String str) throws IOException;

}
