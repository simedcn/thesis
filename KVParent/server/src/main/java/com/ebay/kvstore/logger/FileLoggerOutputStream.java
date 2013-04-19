package com.ebay.kvstore.logger;

import java.io.IOException;
import java.io.OutputStream;

public class FileLoggerOutputStream implements ILoggerOutputStream {

	protected OutputStream out;
	
	private byte writeBuffer[] = new byte[8];

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

	@Override
	public void writeLong(long v) throws IOException {
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
		out.write(writeBuffer, 0, 8);
	}
}
