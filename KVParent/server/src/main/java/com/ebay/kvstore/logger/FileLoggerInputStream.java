package com.ebay.kvstore.logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class FileLoggerInputStream implements ILoggerInputStream {

	protected InputStream in;

	public FileLoggerInputStream(InputStream in) throws IOException {
		this.in = in;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public byte read() throws IOException {
		int ch = in.read();
		if (ch < 0)
			throw new EOFException();
		return (byte) ch;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return in.read(b, 0, b.length);
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
	public String readUTF() throws IOException {
		int length = readInt();
		byte[] bytes = new byte[length];
		read(bytes);
		return new String(bytes);
	}

}