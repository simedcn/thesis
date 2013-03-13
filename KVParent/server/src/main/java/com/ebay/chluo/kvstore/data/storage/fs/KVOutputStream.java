package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
/**
 * 
 * @author luochen
 *
 */
public class KVOutputStream extends FilterOutputStream implements BlockOutputStream {

	protected final int blockSize;

	protected int currentBlock;

	protected int blockPos;

	public KVOutputStream(OutputStream out, int blockSize) {
		super(out);
		this.blockSize = blockSize;
		this.currentBlock = 0;
		this.blockPos = blockSize * currentBlock;
	}

	public int getCurrentBlock() {
		return currentBlock;
	}

	public int getBlockPos() {
		return blockPos;
	}

	public int getPos() {
		return currentBlock * blockSize + blockPos;
	}

	@Override
	public int getBlockAvailable() {
		return blockSize - blockPos;
	}

	/**
	 * Padding 0 if the last block has not been filled.
	 */
	@Override
	public void close() throws IOException {
		int fill = blockSize - blockPos;
		for (int i = 0; i < fill; i++) {
			out.write(0);
		}
		super.close();
	}

	/**
	 * Check the current available space left in a block, and make a new block
	 * if it less than the given len
	 * 
	 * @param len
	 * @throws IOException
	 */
	private void checkBlock(int len) throws IOException {
		int total = blockPos + len;
		if (total >= blockSize) {
			for (int i = 0; i < total - blockSize; i++) {
				out.write(0);
			}
			blockPos = 0;
			currentBlock++;
		}
	}

	/**
	 * This function should be called for each write operation and it will
	 * update the {@link KVOutputStream#currentBlock} and
	 * {@link KVOutputStream#blockPos}
	 * 
	 * @param len
	 */
	private void updateBlock(int len) {
		int pos = currentBlock * blockSize + blockPos + len;
		currentBlock = pos / blockSize;
		blockPos = pos % blockSize;
	}

	@Override
	public void write(byte[] b) throws IOException {
		super.write(b);
		updateBlock(b.length);
	}

	@Override
	public void writeByte(int v) throws IOException {
		checkBlock(1);
		super.write(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		checkBlock(4);
		out.write((v >>> 24) & 0xFF);
		out.write((v >>> 16) & 0xFF);
		out.write((v >>> 8) & 0xFF);
		out.write((v >>> 0) & 0xFF);
		blockPos = blockPos + 4;
	}

	// The following methods are not needed by our current implementation

	@Override
	public void writeLong(long v) throws IOException {

	}

	@Override
	public void writeBoolean(boolean v) throws IOException {

	}

	@Override
	public void writeShort(int v) throws IOException {

	}

	@Override
	public void writeChar(int v) throws IOException {

	}

	@Override
	public void writeFloat(float v) throws IOException {

	}

	@Override
	public void writeDouble(double v) throws IOException {

	}

	@Override
	public void writeBytes(String s) throws IOException {

	}

	@Override
	public void writeChars(String s) throws IOException {

	}

	@Override
	public void writeUTF(String s) throws IOException {

	}

}
