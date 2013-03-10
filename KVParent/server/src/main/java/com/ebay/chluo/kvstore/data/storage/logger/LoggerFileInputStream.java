package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class LoggerFileInputStream implements LoggerInputStream {

	protected FileInputStream in;

	public LoggerFileInputStream(File file) throws IOException {
		this.in = new FileInputStream(file);
	}
	
	public LoggerFileInputStream(FileInputStream in) {
		this.in = in;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public int readInt() throws IOException {
		int ch1 = in.read();
		int ch2 = in.read();
		int ch3 = in.read();
		int ch4 = in.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));

	}

	@Override
	public byte read() throws IOException {
		int ch = in.read();
		if (ch < 0)
			throw new EOFException();
		return (byte) (ch);
	}

	@Override
	public int read(byte[] b) throws IOException {
		return in.read(b, 0, b.length);
	}

}
