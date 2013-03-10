package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.FileOutputStream;
import java.io.IOException;

public class LoggerFileOutputStream implements LoggerOutputStream {

	protected FileOutputStream out;

	@Override
	public void close() throws IOException {
		// End of File
		out.write(-1);
		out.close();
	}

	@Override
	public void writeInt(int i) throws IOException {
		out.write((i >>> 24) & 0xFF);
		out.write((i >>> 16) & 0xFF);
		out.write((i >>> 8) & 0xFF);
		out.write((i >>> 0) & 0xFF);
	}

	@Override
	public void write(byte b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
	}

	public LoggerFileOutputStream(FileOutputStream out) {
		this.out = out;
	}

}
