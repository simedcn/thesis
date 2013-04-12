package com.ebay.kvstore.logger;

import java.io.IOException;
import java.io.OutputStream;

public class FileLoggerOutputStream implements ILoggerOutputStream {

	protected OutputStream out;

	public FileLoggerOutputStream(OutputStream out) {
		this.out = out;
	}

	@Override
	public void close() throws IOException {
		// End of File
		out.close();
	}


	@Override
	public void write(byte b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
	}

	@Override
	public void writeInt(int i) throws IOException {
		out.write((i >>> 24) & 0xFF);
		out.write((i >>> 16) & 0xFF);
		out.write((i >>> 8) & 0xFF);
		out.write((i >>> 0) & 0xFF);
	}

	@Override
	public void writeUTF(String str) throws IOException {
		byte[] bytes = str.getBytes();
		writeInt(bytes.length);
		out.write(bytes);
	}

}
